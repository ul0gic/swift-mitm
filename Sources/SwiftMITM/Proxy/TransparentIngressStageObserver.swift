enum TransparentIngressStage: Sendable {
    case proxyHeaderPending
    case classificationPending
}

protocol TransparentIngressStageObserver: Sendable {
    func didEnterTransparentIngressStage(_ stage: TransparentIngressStage)
}
