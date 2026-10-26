public struct InspectionConfiguration: Sendable {
    public let budget: InspectionMemoryBudget
    public let maximumQueuedBytes: Int
    public let maximumQueuedItems: Int
    public let maximumActiveBodies: Int
    public let maximumLineBytes: Int
    public let maximumEventBytes: Int
    public let maximumMessageBytes: Int
    public let schema: ProtobufSchemaSnapshot?

    public init(
        budget: InspectionMemoryBudget,
        maximumQueuedBytes: Int = 4 * 1024 * 1024,
        maximumQueuedItems: Int = 1024,
        maximumActiveBodies: Int = 256,
        maximumLineBytes: Int = 65_536,
        maximumEventBytes: Int = 1024 * 1024,
        maximumMessageBytes: Int = 4 * 1024 * 1024,
        schema: ProtobufSchemaSnapshot? = nil
    ) throws {
        guard (1...(256 * 1024 * 1024)).contains(maximumQueuedBytes),
              (1...65_536).contains(maximumQueuedItems), (1...1024).contains(maximumActiveBodies),
              (1...262_144).contains(maximumLineBytes), (1...4_194_304).contains(maximumEventBytes),
              (1...16_777_216).contains(maximumMessageBytes)
        else { throw InspectionBudgetError.invalidLimit }
        self.budget = budget
        self.maximumQueuedBytes = maximumQueuedBytes
        self.maximumQueuedItems = maximumQueuedItems
        self.maximumActiveBodies = maximumActiveBodies
        self.maximumLineBytes = maximumLineBytes
        self.maximumEventBytes = maximumEventBytes
        self.maximumMessageBytes = maximumMessageBytes
        self.schema = schema
    }
}
