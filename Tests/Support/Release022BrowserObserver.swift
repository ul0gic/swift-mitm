import Foundation
import Security
import WebKit

@MainActor
final class Release022BrowserObserver: NSObject, WKNavigationDelegate, WKScriptMessageHandler {
    private let root: SecCertificate
    private let completion: Phase2FixtureCompletion<String>

    init(root: SecCertificate, completion: Phase2FixtureCompletion<String>) {
        self.root = root
        self.completion = completion
    }

    func userContentController(_ userContentController: WKUserContentController, didReceive message: WKScriptMessage) {
        guard message.body as? String == "passed" else {
            completion.complete(.failure(Phase2FixtureError.unexpectedBytes))
            return
        }
        completion.complete(.success("passed"))
    }

    func webView(_ webView: WKWebView, didFail navigation: WKNavigation?, withError error: Error) {
        completion.complete(.failure(error))
    }

    func webView(_ webView: WKWebView, didFailProvisionalNavigation navigation: WKNavigation?, withError error: Error) {
        completion.complete(.failure(error))
    }

    func webView(
        _ webView: WKWebView,
        didReceive challenge: URLAuthenticationChallenge,
        completionHandler: @escaping (URLSession.AuthChallengeDisposition, URLCredential?) -> Void
    ) {
        guard challenge.protectionSpace.authenticationMethod == NSURLAuthenticationMethodServerTrust,
              let trust = challenge.protectionSpace.serverTrust,
              SecTrustSetAnchorCertificates(trust, [root] as CFArray) == errSecSuccess,
              SecTrustSetAnchorCertificatesOnly(trust, true) == errSecSuccess,
              SecTrustEvaluateWithError(trust, nil) else {
            completionHandler(.cancelAuthenticationChallenge, nil)
            return
        }
        completionHandler(.useCredential, URLCredential(trust: trust))
    }
}

final class Release022RunnerTrust {
    private let certificate: URL
    private let keychain: URL
    private var previousKeychains: [String] = []
    private var created = false
    private var trusted = false

    init(certificate: URL, directory: URL) {
        self.certificate = certificate
        keychain = directory.appendingPathComponent("browser.keychain-db")
    }

    func install() throws {
        let environment = ProcessInfo.processInfo.environment
        guard environment["GITHUB_ACTIONS"] == "true", environment["RUNNER_ENVIRONMENT"] == "github-hosted" else {
            throw Release022ClientError.missingEnvironment("ephemeral GitHub-hosted runner required for browser CA trust")
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
        _ = try security(["add-trusted-cert", "-r", "trustRoot", "-k", keychain.path, certificate.path])
        trusted = true
    }

    func restore() throws {
        var failure: (any Error)?
        if trusted {
            do {
                _ = try security(["remove-trusted-cert", certificate.path])
                trusted = false
            } catch { failure = error }
        }
        if created {
            do {
                _ = try security(["list-keychains", "-d", "user", "-s"] + previousKeychains)
            } catch { if failure == nil { failure = error } }
            do {
                _ = try security(["delete-keychain", keychain.path])
                created = false
            } catch { if failure == nil { failure = error } }
        }
        if let failure { throw failure }
    }

    private func security(_ arguments: [String]) throws -> String {
        try release022RunProcess(executable: "/usr/bin/security", arguments: arguments)
    }
}
