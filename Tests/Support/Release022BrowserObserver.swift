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
            FileHandle.standardError
                .write(Data("WKWebView anchor setup: anchor=\(anchor), exclusive=\(exclusive)\n".utf8))
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
    private let keychain = "/Library/Keychains/System.keychain"
    private var rootIdentity: (certificate: SecCertificate, fingerprint: String)?
    private var certificateAttempted = false
    private var trustAttempted = false

    init(certificate: URL) {
        self.certificate = certificate
    }

    func install() throws {
        let environment = ProcessInfo.processInfo.environment
        guard environment["GITHUB_ACTIONS"] == "true", environment["RUNNER_ENVIRONMENT"] == "github-hosted" else {
            throw Release022ClientError
                .missingEnvironment("ephemeral GitHub-hosted runner required for browser CA trust")
        }
        rootIdentity = try release022RunnerRoot(certificate: certificate)
        certificateAttempted = true
        trustAttempted = true
        _ = try adminSecurity([
            "add-trusted-cert",
            "-d",
            "-r",
            "trustRoot",
            "-p",
            "ssl",
            "-k",
            keychain,
            certificate.path
        ])
        try release022ReportRunnerTrust(certificate: certificate)
    }

    func restore() throws {
        guard let rootIdentity else { return }
        var failures: [any Error] = []
        if trustAttempted {
            do {
                _ = try adminSecurity(["remove-trusted-cert", "-d", certificate.path])
                try release022RequireRunnerTrustAbsent(rootIdentity.certificate)
                trustAttempted = false
            } catch { failures.append(error) }
        }
        if certificateAttempted {
            do {
                _ = try adminSecurity(["delete-certificate", "-Z", rootIdentity.fingerprint, keychain])
                try requireCertificateAbsent(fingerprint: rootIdentity.fingerprint)
                certificateAttempted = false
            } catch { failures.append(error) }
        }
        if !trustAttempted, !certificateAttempted {
            self.rootIdentity = nil
        }
        if let failure = failures.first {
            for secondary in failures.dropFirst() {
                FileHandle.standardError.write(Data("Chromium secondary trust cleanup failure: \(secondary)\n".utf8))
            }
            throw failure
        }
    }

    private func requireCertificateAbsent(fingerprint: String) throws {
        let output = try security(["find-certificate", "-a", "-Z", keychain])
        let present = output.split(separator: "\n").contains {
            $0 == "SHA-256 hash: " + fingerprint.uppercased()
        }
        FileHandle.standardError.write(Data("Chromium certificate cleanup: present=\(present)\n".utf8))
        guard !present else {
            throw Release022ClientError.commandFailed("generated root remains in System.keychain")
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
