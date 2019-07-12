package Config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

public class ConfigurationImpl implements Configuration, Serializable {

    /**
     * 项目配置
     */
    private Properties properties;

    /**
     * 日志文件
     */
    private Logger logger = LoggerFactory.getLogger(Configuration.class);

    private static volatile ConfigurationImpl config;

    public static ConfigurationImpl getInstance(){
        if(config == null){
            synchronized (ConfigurationImpl.class){
                if(config == null){
                    config = new ConfigurationImpl();
                }
            }
        }
        return config;
    }


    public void init(){
        try {
            properties = new Properties();
            InputStream in = ConfigurationImpl.class.getResourceAsStream("/config.properties");
            properties.load(in);
        }catch (Exception e){
            logger.error(e.getMessage());
        }
    }

    @Override
    public String getBrokerList(){
        return properties.getProperty("kafka.brokerlist");
    }

    @Override
    public String getGroupId() {
        return properties.getProperty("kafka.groupId");
    }

    @Override
    public String getTopic() {
        return properties.getProperty("kafka.Topic");
    }

}
