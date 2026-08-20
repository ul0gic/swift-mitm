enum TransparentIngressStage: Sendable {
    case proxyHeaderPending
    case classificationPending
    case opaqueBridgeReady
}

protocol TransparentIngressStageObserver: Sendable {
    func didEnterTransparentIngressStage(_ stage: TransparentIngressStage)
}
