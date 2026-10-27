import Darwin

enum Release022RunnerInterfaceError: Error {
    case enumeration(Int32)
    case numericAddress(Int32)
    case noNonLoopbackIPv4
}

func release022RunnerIPv4() throws -> String {
    var interfaces: UnsafeMutablePointer<ifaddrs>?
    guard getifaddrs(&interfaces) == 0 else { throw Release022RunnerInterfaceError.enumeration(errno) }
    defer { freeifaddrs(interfaces) }
    var addresses: [String] = []
    var cursor = interfaces
    while let interface = cursor {
        defer { cursor = interface.pointee.ifa_next }
        let flags = interface.pointee.ifa_flags
        guard flags & UInt32(IFF_UP | IFF_RUNNING) == UInt32(IFF_UP | IFF_RUNNING),
              flags & UInt32(IFF_LOOPBACK | IFF_POINTOPOINT) == 0,
              let address = interface.pointee.ifa_addr,
              address.pointee.sa_family == sa_family_t(AF_INET) else { continue }
        var host = [CChar](repeating: 0, count: Int(NI_MAXHOST))
        let capacity = socklen_t(host.count)
        let status = getnameinfo(
            address,
            socklen_t(address.pointee.sa_len),
            &host,
            capacity,
            nil,
            0,
            NI_NUMERICHOST
        )
        guard status == 0 else { throw Release022RunnerInterfaceError.numericAddress(status) }
        guard let end = host.firstIndex(of: 0),
              let numeric = String(validating: host[..<end], as: UTF8.self) else {
            throw Release022RunnerInterfaceError.numericAddress(EAI_FAIL)
        }
        let octets = numeric.split(separator: ".").compactMap { UInt8($0) }
        guard octets.count == 4, octets[0] > 0, octets[0] < 224, octets[0] != 127,
              !(octets[0] == 169 && octets[1] == 254) else { continue }
        addresses.append(numeric)
    }
    guard let address = addresses.min() else { throw Release022RunnerInterfaceError.noNonLoopbackIPv4 }
    return address
}
