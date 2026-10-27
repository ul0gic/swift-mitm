extension HTTPControlDecision {
    public func effectiveTrailers(_ fields: [HTTPHeaderField]) -> [HTTPHeaderField] {
        switch action {
        case .replaceBody:
            HTTPControlConfiguration.removingReplacedBodyFields(fields)
        case .respond:
            []
        default:
            fields
        }
    }
}
