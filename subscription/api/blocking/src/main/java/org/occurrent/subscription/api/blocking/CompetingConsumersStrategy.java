package org.occurrent.subscription.api.blocking;

public interface CompetingConsumersStrategy {
    boolean registerCompetingConsumer(String subscriptionId, String subscriberId);

    void unregisterCompetingConsumer(String subscriptionId, String subscriberId);

    boolean isRegisteredCompetingConsumer(String subscriptionId, String subscriberId);

    void addListener(CompetingConsumerListener listenerConsumer);

    void removeListener(CompetingConsumerListener listenerConsumer);

    interface CompetingConsumerListener {
        default void onConsumeGranted(String subscriptionId, String subscriberId) {
        }

        default void onConsumeProhibited(String subscriptionId, String subscriberId) {
        }
    }
}