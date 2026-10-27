import CFNetwork
import Foundation
import Network
import NIOConcurrencyHelpers
import NIOSSL
import Security
import XCTest

import SwiftMITM

enum Release022ProxyRouting: String {
    case native
    case nativeAlternateLoader
    case sessionDictionary
}

func release022TrustedURLSession(
    port: Int,
    authority: CertificateAuthority,
    originHost: String,
    originPort: Int,
    protocolName: String,
    routing: Release022ProxyRouting
) throws -> URLSession {
    let configuration = URLSessionConfiguration.ephemeral
    configuration.timeoutIntervalForRequest = 10
    let preparedDefault = configuration.usesClassicLoadingMode
    if routing == .nativeAlternateLoader {
        configuration.usesClassicLoadingMode = !preparedDefault
    }
    if routing != .sessionDictionary {
        let proxyPort = try XCTUnwrap(UInt16(exactly: port))
        let endpointPort = try XCTUnwrap(NWEndpoint.Port(rawValue: proxyPort))
        let proxyConfiguration = ProxyConfiguration(httpCONNECTProxy: .hostPort(host: "127.0.0.1", port: endpointPort))
        let native = proxyConfiguration._nw
        nw_proxy_config_clear_match_domains(native)
        nw_proxy_config_clear_excluded_domains(native)
        originHost.withCString { nw_proxy_config_add_match_domain(native, $0) }
        nw_proxy_config_set_failover_allowed(native, false)
        release022AssertNativeProxyConfiguration(proxyConfiguration, originHost: originHost, phase: "prepared")
        configuration.proxyConfigurations = [proxyConfiguration]
    } else {
        configuration.connectionProxyDictionary = [
            kCFNetworkProxiesHTTPSEnable as String: 1,
            kCFNetworkProxiesHTTPSProxy as String: "127.0.0.1",
            kCFNetworkProxiesHTTPSPort as String: port
        ]
    }
    let der = try NIOSSLCertificate(bytes: Array(authority.caCertificatePEM.utf8), format: .pem).toDERBytes()
    let root = try XCTUnwrap(SecCertificateCreateWithData(nil, Data(der) as CFData))
    let delegate = Release022URLSessionTrust(root: root, protocolName: protocolName, routing: routing.rawValue)
    let session = URLSession(configuration: configuration, delegate: delegate, delegateQueue: nil)
    release022AssertURLSessionLoader(configuration, session: session, preparedDefault: preparedDefault, routing: routing)
    let copied = session.configuration.proxyConfigurations
    if routing != .sessionDictionary {
        XCTAssertEqual(copied.count, 1)
        for proxy in copied.prefix(4) {
            release022AssertNativeProxyConfiguration(proxy, originHost: originHost, phase: "copied")
        }
    } else {
        XCTAssertTrue(copied.isEmpty)
        release022AssertSessionProxyDictionary(session.configuration, port: port)
    }
    release022ReportURLSessionConfiguration(session, proxyPort: port, originHost: originHost, originPort: originPort)
    return session
}

private func release022AssertURLSessionLoader(
    _ configuration: URLSessionConfiguration,
    session: URLSession,
    preparedDefault: Bool,
    routing: Release022ProxyRouting
) {
    let selected = configuration.usesClassicLoadingMode
    let copied = session.configuration.usesClassicLoadingMode
    XCTAssertEqual(selected, routing == .nativeAlternateLoader ? !preparedDefault : preparedDefault)
    XCTAssertEqual(copied, selected)
    let facts = "routing=\(routing.rawValue), preparedDefault=\(preparedDefault), "
        + "selected=\(selected), copied=\(copied)"
    FileHandle.standardError.write(Data("URLSession loader configuration: \(facts)\n".utf8))
}

private func release022ReportURLSessionConfiguration(
    _ session: URLSession,
    proxyPort: Int,
    originHost: String,
    originPort: Int
) {
    let proxies = session.configuration.proxyConfigurations
    let endpoints = "expectedProxy=127.0.0.1:\(proxyPort), expectedOrigin=\(originHost):\(originPort)"
    let configuration = "URLSession copied configuration: \(endpoints), proxies=\(proxies.count)\n"
    FileHandle.standardError.write(Data(configuration.utf8))
    for (index, proxy) in proxies.prefix(4).enumerated() {
        let matches = proxy.matchDomains.prefix(4).map { String($0.prefix(128)) }
        let exclusions = proxy.excludedDomains.prefix(4).map { String($0.prefix(128)) }
        let facts = "proxy=\(index), failover=\(proxy.allowFailover), "
            + "matchCount=\(proxy.matchDomains.count), matches=\(matches), "
            + "excludedCount=\(proxy.excludedDomains.count), exclusions=\(exclusions)"
        FileHandle.standardError.write(Data("URLSession copied proxy: \(facts)\n".utf8))
    }
}

private func release022AssertNativeProxyConfiguration(_ proxy: ProxyConfiguration, originHost: String, phase: String) {
    var matches: [String?] = []
    var exclusions: [String?] = []
    nw_proxy_config_enumerate_match_domains(proxy._nw) { matches.append(String(validatingCString: $0)) }
    nw_proxy_config_enumerate_excluded_domains(proxy._nw) { exclusions.append(String(validatingCString: $0)) }
    let failover = nw_proxy_config_get_failover_allowed(proxy._nw)
    XCTAssertEqual(matches, [Optional(originHost)])
    XCTAssertEqual(exclusions, [])
    XCTAssertFalse(failover)
    let shownMatches = matches.prefix(4).map { $0?.prefix(128) }
    let shownExclusions = exclusions.prefix(4).map { $0?.prefix(128) }
    let facts = "phase=\(phase), failover=\(failover), matchCount=\(matches.count), matches=\(shownMatches), "
        + "excludedCount=\(exclusions.count), exclusions=\(shownExclusions)"
    FileHandle.standardError.write(Data("URLSession native proxy: \(facts)\n".utf8))
}

func release022AssertURLSessionBody(_ session: URLSession, host: String, port: Int) async throws {
    let url = try XCTUnwrap(URL(string: "https://\(host):\(port)/"))
    let (data, response) = try await session.data(from: url)
    XCTAssertEqual(data, Data(repeating: 0x41, count: 32))
    XCTAssertEqual((response as? HTTPURLResponse)?.statusCode, 200)
}

func release022AssertURLSessionRouting(_ session: URLSession) throws {
    let delegate = try XCTUnwrap(session.delegate as? Release022URLSessionTrust)
    delegate.assertObservedRoute()
}

private func release022AssertSessionProxyDictionary(_ configuration: URLSessionConfiguration, port: Int) {
    let dictionary = configuration.connectionProxyDictionary
    let enabled = dictionary?[kCFNetworkProxiesHTTPSEnable as String] as? Int
    let host = dictionary?[kCFNetworkProxiesHTTPSProxy as String] as? String
    let copiedPort = dictionary?[kCFNetworkProxiesHTTPSPort as String] as? Int
    XCTAssertEqual(enabled, 1)
    XCTAssertEqual(host, "127.0.0.1")
    XCTAssertEqual(copiedPort, port)
    let facts = "enabled=\(String(describing: enabled)), host=\(String(describing: host?.prefix(128))), "
        + "port=\(String(describing: copiedPort))"
    FileHandle.standardError.write(Data("URLSession copied HTTPS proxy dictionary: \(facts)\n".utf8))
}

private final class Release022URLSessionTrust: NSObject, URLSessionTaskDelegate, @unchecked Sendable {
    private struct RouteObservation { var received = false; var matches = false }
    private let observedRoute = NIOLockedValueBox(RouteObservation())
    let root: SecCertificate
    let protocolName: String
    let routing: String

    init(root: SecCertificate, protocolName: String, routing: String) {
        self.root = root
        self.protocolName = protocolName
        self.routing = routing
    }

    func urlSession(_ session: URLSession, task: URLSessionTask, didFinishCollecting metrics: URLSessionTaskMetrics) {
        let matches = metrics.transactionMetrics.contains {
            $0.isProxyConnection && (protocolName != "h2" || $0.networkProtocolName == "h2")
        }
        observedRoute.withLockedValue { $0 = RouteObservation(received: true, matches: matches) }
        XCTAssertTrue(matches, "URLSession \(routing) must observe the configured proxy and expected protocol")
        for (index, transaction) in metrics.transactionMetrics.prefix(4).enumerated() {
            let facts = "routing=\(routing), transaction=\(index), proxy=\(transaction.isProxyConnection), "
                + "reused=\(transaction.isReusedConnection), "
                + "protocol=\(String(describing: transaction.networkProtocolName?.prefix(32))), "
                + "localAddress=\(String(describing: transaction.localAddress?.prefix(128))), "
                + "localPort=\(String(describing: transaction.localPort)), "
                + "remoteAddress=\(String(describing: transaction.remoteAddress?.prefix(128))), "
                + "remotePort=\(String(describing: transaction.remotePort))"
            FileHandle.standardError.write(Data("URLSession route metrics: \(facts)\n".utf8))
        }
    }

    func assertObservedRoute() {
        let observed = observedRoute.withLockedValue { $0 }
        guard observed.received else {
            XCTFail("URLSession \(routing) routing proof unavailable: metrics callback not observed")
            return
        }
        XCTAssertTrue(observed.matches, "URLSession \(routing) observed metrics did not prove proxy and protocol")
    }

    func urlSession(
        _ session: URLSession,
        didReceive challenge: URLAuthenticationChallenge,
        completionHandler: @escaping (URLSession.AuthChallengeDisposition, URLCredential?) -> Void
    ) {
        guard challenge.protectionSpace.authenticationMethod == NSURLAuthenticationMethodServerTrust else {
            XCTFail("URLSession unexpected authentication method: \(challenge.protectionSpace.authenticationMethod)")
            completionHandler(.cancelAuthenticationChallenge, nil)
            return
        }
        guard let trust = challenge.protectionSpace.serverTrust else {
            XCTFail("URLSession server-trust challenge omitted trust")
            completionHandler(.cancelAuthenticationChallenge, nil)
            return
        }
        let anchorStatus = SecTrustSetAnchorCertificates(trust, [root] as CFArray)
        let exclusiveStatus = SecTrustSetAnchorCertificatesOnly(trust, true)
        guard anchorStatus == errSecSuccess, exclusiveStatus == errSecSuccess else {
            XCTFail("URLSession custom-root setup failed: anchor=\(anchorStatus), exclusive=\(exclusiveStatus)")
            completionHandler(.cancelAuthenticationChallenge, nil)
            return
        }
        var failure: CFError?
        guard SecTrustEvaluateWithError(trust, &failure) else {
            let facts = release022TrustDiagnostic(trust: trust, expectedRoot: root, failure: failure)
            XCTFail("URLSession trust denied: \(facts); host=\(challenge.protectionSpace.host)")
            completionHandler(.cancelAuthenticationChallenge, nil)
            return
        }
        let expectedDER = SecCertificateCopyData(root) as Data
        guard let chain = SecTrustCopyCertificateChain(trust) as? [SecCertificate],
              chain.contains(where: { SecCertificateCopyData($0) as Data == expectedDER }) else {
            XCTFail("URLSession \(routing) evaluated chain omitted expected MITM root")
            completionHandler(.cancelAuthenticationChallenge, nil)
            return
        }
        let facts = "routing=\(routing), expectedRootPresent=true, chainCount=\(chain.count)"
        FileHandle.standardError.write(Data("URLSession evaluated trust: \(facts)\n".utf8))
        completionHandler(.useCredential, URLCredential(trust: trust))
    }
}
