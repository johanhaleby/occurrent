package org.occurrent.subscription.api.blocking;

public interface CompetingConsumersStrategy {
    boolean registerCompetingConsumer(String subscriptionId, String subscriberId);

    void unregisterCompetingConsumer(String subscriptionId, String subscriberId);

    void addListener(CompetingConsumerListener listenerConsumer);

    class NoopCompetingConsumerStrategy implements CompetingConsumersStrategy {
        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            return true;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {

        }

        @Override
        public void addListener(CompetingConsumerListener listenerConsumer) {

        }
    }

    interface CompetingConsumerListener {
        default void onConsumeGranted(String subscriptionId, String subscriberId) {
        }

        default void onConsumeProhibited(String subscriptionId, String subscriberId) {
        }
    }
}
