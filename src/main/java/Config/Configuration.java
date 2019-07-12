package Config;

public interface Configuration {
    /**
     * 获取kafka的broker list
     */
    String getBrokerList();

    /**
     * 获取kafka的topic
     */
    String getTopic();

    /**
     * 获取kafka的消费group id
     */
    String getGroupId();
}
