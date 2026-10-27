import Foundation
import Security
import WebKit

@MainActor
final class Release022BrowserObserver: NSObject, WKNavigationDelegate, WKScriptMessageHandler {
    private let root: SecCertificate
    private let completion: Phase2FixtureCompletion<String>
    private var stage = "created"

    init(root: SecCertificate, completion: Phase2FixtureCompletion<String>) {
        self.root = root
        self.completion = completion
    }

    func userContentController(_ userContentController: WKUserContentController, didReceive message: WKScriptMessage) {
        stage = "application-result"
        guard message.body as? String == "passed" else {
            completion.complete(.failure(Phase2FixtureError.unexpectedBytes))
            return
        }
        completion.complete(.success("passed"))
    }

    func webView(_ webView: WKWebView, didFail navigation: WKNavigation?, withError error: Error) {
        reportNavigationFailure(error)
        completion.complete(.failure(error))
    }

    func webView(_ webView: WKWebView, didFailProvisionalNavigation navigation: WKNavigation?, withError error: Error) {
        reportNavigationFailure(error)
        completion.complete(.failure(error))
    }

    func webView(_ webView: WKWebView, didStartProvisionalNavigation navigation: WKNavigation?) {
        stage = "provisional-navigation"
    }

    func webView(_ webView: WKWebView, didCommit navigation: WKNavigation?) { stage = "committed-navigation" }

    func webView(_ webView: WKWebView, didFinish navigation: WKNavigation?) { stage = "finished-navigation" }

    func webView(
        _ webView: WKWebView,
        didReceive challenge: URLAuthenticationChallenge,
        completionHandler: @escaping @MainActor @Sendable (URLSession.AuthChallengeDisposition, URLCredential?) -> Void
    ) {
        stage = "server-trust"
        guard challenge.protectionSpace.authenticationMethod == NSURLAuthenticationMethodServerTrust,
              let trust = challenge.protectionSpace.serverTrust else {
            FileHandle.standardError.write(Data("WKWebView challenge omitted server trust\n".utf8))
            completionHandler(.cancelAuthenticationChallenge, nil)
            return
        }
        let anchor = SecTrustSetAnchorCertificates(trust, [root] as CFArray)
        let exclusive = SecTrustSetAnchorCertificatesOnly(trust, true)
        guard anchor == errSecSuccess, exclusive == errSecSuccess else {
            FileHandle.standardError.write(Data("WKWebView anchor setup: anchor=\(anchor), exclusive=\(exclusive)\n".utf8))
            completionHandler(.cancelAuthenticationChallenge, nil)
            return
        }
        var failure: CFError?
        guard SecTrustEvaluateWithError(trust, &failure) else {
            let facts = release022TrustDiagnostic(trust: trust, expectedRoot: root, failure: failure)
            FileHandle.standardError.write(Data("WKWebView trust denied: \(facts)\n".utf8))
            completionHandler(.cancelAuthenticationChallenge, nil)
            return
        }
        stage = "server-trust-accepted"
        completionHandler(.useCredential, URLCredential(trust: trust))
    }

    private func reportNavigationFailure(_ error: Error) {
        let facts = release022NavigationErrorDiagnostic(error)
        FileHandle.standardError.write(Data("WKWebView stage=\(stage): \(facts)\n".utf8))
    }
}

final class Release022RunnerTrust {
    private let certificate: URL
    private let keychain: URL
    private var previousKeychains: [String] = []
    private var created = false
    private var trustAttempted = false

    init(certificate: URL, directory: URL) {
        self.certificate = certificate
        keychain = directory.appendingPathComponent("browser.keychain-db")
    }

    func install() throws {
        let environment = ProcessInfo.processInfo.environment
        guard environment["GITHUB_ACTIONS"] == "true", environment["RUNNER_ENVIRONMENT"] == "github-hosted" else {
            throw Release022ClientError
                .missingEnvironment("ephemeral GitHub-hosted runner required for browser CA trust")
        }
        let list = try security(["list-keychains", "-d", "user"])
        previousKeychains = list.split(separator: "\n").map {
            $0.trimmingCharacters(in: .whitespacesAndNewlines).trimmingCharacters(in: CharacterSet(charactersIn: "\""))
        }
        let password = UUID().uuidString
        _ = try security(["create-keychain", "-p", password, keychain.path])
        created = true
        _ = try security(["unlock-keychain", "-p", password, keychain.path])
        _ = try security(["list-keychains", "-d", "user", "-s", keychain.path] + previousKeychains)
        trustAttempted = true
        _ = try adminSecurity([
            "add-trusted-cert",
            "-d",
            "-r",
            "trustRoot",
            "-p",
            "ssl",
            "-k",
            keychain.path,
            certificate.path
        ])
    }

    func restore() throws {
        var failure: any Error?
        if trustAttempted {
            do {
                _ = try adminSecurity(["remove-trusted-cert", "-d", certificate.path])
                trustAttempted = false
            } catch { failure = error }
        }
        if created {
            do {
                _ = try security(["list-keychains", "-d", "user", "-s"] + previousKeychains)
            } catch {
                if failure == nil {
                    failure = error
                }
            }
            do {
                _ = try security(["delete-keychain", keychain.path])
                created = false
            } catch {
                if failure == nil {
                    failure = error
                }
            }
        }
        if let failure {
            throw failure
        }
    }

    private func security(_ arguments: [String]) throws -> String {
        try release022RunProcess(
            executable: "/usr/bin/security",
            arguments: arguments,
            timeout: 30,
            phase: "security \(arguments[0])"
        )
    }

    private func adminSecurity(_ arguments: [String]) throws -> String {
        try release022RunProcess(
            executable: "/usr/bin/sudo",
            arguments: ["-n", "/usr/bin/security"] + arguments,
            timeout: 30,
            phase: "security-admin \(arguments[0])"
        )
    }
}
