/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 *
 * CloudWoodMQTT watches iot/#
 * Messages should be in format iot/[thing]/
 *
 */
package cloudwoodmqtt;

import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttTopic;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.SimpleFormatter;

/**
 *
 * @author jmccartney
 */
public class CloudWoodMQTT implements MqttCallback {

    //mqtt connection
    MqttClient myClient;
    MqttConnectOptions connOpt;

    //mysql connection
    static Connection con = null;

    //our logger
    private static final Logger logger = Logger.getLogger(CloudWoodMQTT.class.getName());
    private static FileHandler handler;

    //our configuration file configuration.properties
    static Properties prop = new Properties();

    /**
     *
     * connectionLost This callback is invoked upon losing the MQTT connection.
     *
     * @param t
     */
    @Override
    public void connectionLost(Throwable t) {
        logger.log(Level.SEVERE, "connection to mqtt lost: {0}", t.getStackTrace());
        // code to reconnect to the broker would go here if desired
    }

    /**
     *
     * deliveryComplete This callback is invoked when a message published by
     * this client is successfully received by the broker.
     *
     */
    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        //System.out.println("Pub complete" + new String(token.getMessage().getPayload()));
    }

    /**
     *
     * messageArrived This callback is invoked when a message is received on a
     * subscribed topic.
     *
     */
    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
//        System.out.println("-------------------------------------------------");
//        System.out.println("| Topic:" + topic);
//        System.out.println("| Message: " + new String(message.getPayload()));
//        System.out.println("| QOS: " + message.getQos());
//        System.out.println("| Duplicate: " + message.isDuplicate());
//        System.out.println("-------------------------------------------------");

        PreparedStatement pst = null;
        java.util.Date date = new Date();
        Timestamp timestamp = new Timestamp(date.getTime());

        //
        //get user id from topic
        //
        //get name from topic
        String name = splitPath(topic)[1];

        Statement st;
        ResultSet rs;
        int userId = 0;
        try {
            st = con.createStatement();
            rs = st.executeQuery("SELECT id FROM user WHERE name = '" + name + "'");

            if (rs.next()) {
                userId = Integer.parseInt(rs.getString(1));
                //update user used timestamp
                st.executeQuery("UPDATE user SET last_broadcast='" + timestamp + "' WHERE id = '" + userId + "'");
            } else {
                //we need to create one (update timestamp as well)
                pst = con.prepareStatement("INSERT INTO user(last_broadcast,name) VALUES(?,?)", Statement.RETURN_GENERATED_KEYS);
                pst.setTimestamp(1, timestamp);
                pst.setString(2, name);
                pst.executeUpdate();
                rs = pst.getGeneratedKeys();
                if (rs.next()) {
                    userId = rs.getInt(1);
                }
            }

        } catch (SQLException ex) {
            //System.out.println(ex.getMessage());
            logger.log(Level.SEVERE, "mysql error: {0}", ex.getMessage());
        }

        //
        //get topic id from topic
        //
        int topicId = 0;
        try {
            st = con.createStatement();
            rs = st.executeQuery("SELECT id FROM topic WHERE path = '" + topic + "'");

            if (rs.next()) {
                topicId = Integer.parseInt(rs.getString(1));
                //update topic used timestamp
                st.executeQuery("UPDATE topic SET last_broadcast='" + timestamp + "' WHERE id = '" + topicId + "'");
            } else {
                //we need to create one (update timestamp as well)
                pst = con.prepareStatement("INSERT INTO topic(last_broadcast,path) VALUES(?,?)", Statement.RETURN_GENERATED_KEYS);
                pst.setTimestamp(1, timestamp);
                pst.setString(2, topic);
                pst.executeUpdate();
                rs = pst.getGeneratedKeys();
                if (rs.next()) {
                    topicId = rs.getInt(1);
                }

            }
        } catch (SQLException ex) {
            //System.out.println(ex.getMessage());
            logger.log(Level.SEVERE, "mysql error: {0}", ex.getMessage());
        }

        //
        //do insert into log
        //
        if (userId != 0 && topicId != 0) {
            try {
                pst = con.prepareStatement("INSERT INTO log(user_id,path_id,payload,qos,dupe) VALUES(?,?,?,?,?)");
                pst.setInt(1, userId);
                pst.setInt(2, topicId);
                pst.setString(3, new String(message.getPayload()));
                pst.setInt(4, message.getQos());
                pst.setInt(5, booleanToInt(message.isDuplicate()));
                pst.executeUpdate();

            } catch (SQLException ex) {
                //System.out.println(ex.getMessage());
                logger.log(Level.SEVERE, "mysql error: {0}", ex.getMessage());

            }
        }

    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            handler = new FileHandler("cloudwoodmqtt-log.%u.%g.txt"); 
            logger.addHandler(handler);
            handler.setFormatter(new SimpleFormatter());
            
        } catch (IOException | SecurityException ex) {
            Logger.getLogger(CloudWoodMQTT.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        String fileName = "configuration.properties";
        try {
            InputStream is = new FileInputStream(fileName);
            prop.load(is);
        } catch (IOException ex) {
            logger.log(Level.SEVERE, "error accessing configuration file: {0}", ex.getStackTrace());
        }

        connectMysql();
        Statement st = null;
        ResultSet rs = null;
        try {
            st = con.createStatement();
            rs = st.executeQuery("SELECT VERSION()");
            if (rs.next()) {
                logger.log(Level.INFO, "connected to: {0}", rs.getString(1));
            }

        } catch (SQLException ex) {
            logger.log(Level.SEVERE, "mysql error: {0}", ex.getMessage());
        }

        //load users into memory
        CloudWoodMQTT smc = new CloudWoodMQTT();
        smc.runClient();

    }

    public void runClient() {
        // setup MQTT Client
        String clientID = prop.getProperty("mqtt_thing");
        connOpt = new MqttConnectOptions();

        connOpt.setCleanSession(true);
        connOpt.setKeepAliveInterval(30);
        connOpt.setUserName(prop.getProperty("mqtt_username"));
        connOpt.setPassword(prop.getProperty("mqtt_password_md5").toCharArray());

        // Connect to Broker
        try {
            myClient = new MqttClient(prop.getProperty("broker_url"), clientID);
            myClient.setCallback(this);
            myClient.connect(connOpt);
        } catch (MqttException e) {
            logger.log(Level.SEVERE, "mqtt error connecting to broker: " + prop.getProperty("broker_url"), e);
            //e.printStackTrace();
            System.exit(-1);
        }

        logger.log(Level.INFO, "connected to broker: {0}", prop.getProperty("broker_url"));

        // setup topic
        // topics on m2m.io are in the form <domain>/<stuff>/<thing>
        //String myTopic = prop.getProperty("mqtt_domainl") + "/" + MQTT_STUFF + "/" + prop.getProperty("mqtt_thing");
        String myTopic = prop.getProperty("mqtt_domain");
        MqttTopic topic = myClient.getTopic(myTopic);

        try {
            int subQoS = 1;
            myClient.subscribe(myTopic + "/#", subQoS);
        } catch (Exception e) {
            //e.printStackTrace();
            logger.log(Level.SEVERE, "mqtt error subscribing to topic: " + myTopic + "/#", e);

        }
    }

    public static void connectMysql() {
        try {
            con = DriverManager.getConnection(prop.getProperty("mysql_url"), prop.getProperty("mysql_user"), prop.getProperty("mysql_password"));
        } catch (SQLException ex) {
            //System.out.println(ex.getMessage());
            logger.log(Level.SEVERE, "mysql error: {0}", ex.getMessage());
        }
    }

    public void disconnectMysql() {

    }

    public static String[] splitPath(String path) {
        String backslash = ((char) 92) + "";
        if (path.contains(backslash)) {
            ArrayList<String> parts = new ArrayList<>();
            int start = 0;
            int end = 0;
            for (int c : path.toCharArray()) {
                if (c == 92) {
                    parts.add(path.substring(start, end));
                    start = end + 1;
                }
                end++;
            }
            parts.add(path.substring(start));
            return parts.toArray(new String[parts.size()]);
        }
        return path.split("/");
    }

    public static int booleanToInt(boolean value) {
        // Convert true to 1 and false to 0.
        return value ? 1 : 0;
    }
}
