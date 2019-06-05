package nb.kafka.operator.watch;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static nb.common.App.metrics;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import nb.kafka.operator.util.PropertyUtil;
import nb.kafka.operator.AppConfig;
import nb.kafka.operator.Topic;

import java.util.concurrent.CountDownLatch;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import io.fabric8.kubernetes.client.KubernetesClientException;


public class ConfigMapWatcherTest {

  final String path = "/api/v1/namespaces/test/configmaps?labelSelector=config%3Dkafka-topic&watch=true";
  final CountDownLatch crudLatch = new CountDownLatch(1);

  public KubernetesServer server;

  @BeforeEach
  void setUp() {
    server = new KubernetesServer();
    server.before();
  }

  @AfterEach
  void tearDown() {
    metrics().remove("managed-topics");
    server.after();
    server = null;
  }

  @Test
  void testBuildTopicModel() {
    // Arrange
    String topicName = "test-topic";
    int partitions = 20;
    int replicationFactor = 2;
    String properties = "retention.ms=2000000";

    Map<String, String> data = new HashMap<>();
    data.put("partitions", Integer.toString(partitions));
    data.put("properties", properties);
    data.put("replication-factor", Integer.toString(replicationFactor));

    ObjectMeta metadata = new ObjectMeta();
    metadata.setName(topicName);
    Map<String, String> labels = Collections.singletonMap("config", "kafka-topic");
    metadata.setLabels(labels);
    ConfigMap cm = new ConfigMap("v1", data, "configMap", metadata);

    AppConfig appConfig = new AppConfig();
    appConfig.setKafkaUrl("kafka:9092");
    Map<String, String> standardLabels = Collections.singletonMap("config", "kafka-topic");
    appConfig.setStandardLabels(standardLabels);
    try (ConfigMapWatcher configMapWatcher = new ConfigMapWatcher(null, appConfig)) {
      // Act
      Topic topic = configMapWatcher.buildTopicModel(cm);

      // Assert
      assertNotNull(topic);
      assertEquals(topicName, topic.getName());
      assertEquals(partitions, topic.getPartitions());
      assertEquals(replicationFactor, topic.getReplicationFactor());
      assertEquals("retention.ms", topic.getProperties()
        .entrySet()
        .iterator()
        .next()
        .getKey());
      assertEquals("2000000", topic.getProperties()
        .entrySet()
        .iterator()
        .next()
        .getValue());
    }
  }

  @Test
  void testWatchConfigMapCreationTopic() throws InterruptedException {

    String topicName = "test-toto";
    int partitions = 20;
    int replicationFactor = 3;
    String propsString = "partitions=20,compression.type=producer,retention.ms=3600000,replication-factor=3";
    Map<String, String> data = PropertyUtil.stringToMap(propsString);

    ObjectMeta metadata = new ObjectMeta();

    metadata.setName(topicName);
    Map<String, String> labels = Collections.singletonMap("config", "kafka-topic");
    metadata.setLabels(labels);
    ConfigMap cm2 = new ConfigMap("v1", data, "ConfigMap", metadata);
    cm2.setData(data);

    AppConfig appConfig = new AppConfig();
    Map<String, String> standardLabels = Collections.singletonMap("config", "kafka-topic");
    appConfig.setStandardLabels(standardLabels);
    KubernetesClient client = server.getClient();

    server.expect()
      .withPath(path)
      .andUpgradeToWebSocket().open().waitFor(500).andEmit(new WatchEventBuilder().withConfigMapObject(cm2).withType("ADDED").build()).done().once();
    try (ConfigMapWatcher configMapWatcher = new ConfigMapWatcher(client, appConfig)) {
      configMapWatcher.watch();
      //topic = configMapWatcher.buildTopicModel(cm2);
      configMapWatcher.setOnCreateListener((topic) -> {
        crudLatch.countDown();
        assertNotNull(topic);
        assertEquals(topicName, topic.getName());
        assertEquals(partitions, topic.getPartitions());
        assertEquals(replicationFactor, topic.getReplicationFactor());
      });
      assertTrue(crudLatch.await(10, TimeUnit.SECONDS));

    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Test
  void testWatchConfigMapUpdateTopic() throws InterruptedException {

    String topicName = "test-toto";
    int partitions = 20;
    int replicationFactor = 3;
    String propsString = "partitions=20,compression.type=producer,retention.ms=3600000,replication-factor=3";
    Map<String, String> data = PropertyUtil.stringToMap(propsString);

    ObjectMeta metadata = new ObjectMeta();

    metadata.setName(topicName);
    Map<String, String> labels = Collections.singletonMap("config", "kafka-topic");
    metadata.setLabels(labels);
    ConfigMap cm3 = new ConfigMap("v1", data, "ConfigMap", metadata);
    cm3.setData(data);

    AppConfig appConfig = new AppConfig();
    Map<String, String> standardLabels = Collections.singletonMap("config", "kafka-topic");
    appConfig.setStandardLabels(standardLabels);
    KubernetesClient client = server.getClient();

    server.expect()
      .withPath(path)
      .andUpgradeToWebSocket()
      .open()
      .waitFor(500)
      .andEmit(new WatchEventBuilder()
        .withConfigMapObject(cm3).withType("MODIFIED")
        .build())
      .done()
      .once();

    try (ConfigMapWatcher configMapWatcher = new ConfigMapWatcher(client, appConfig)) {
      configMapWatcher.watch();
      //topic = configMapWatcher.buildTopicModel(cm2);
      configMapWatcher.setOnUpdateListener((topic) -> {
        crudLatch.countDown();
        assertNotNull(topic);
        assertEquals(topicName, topic.getName());
        assertEquals(partitions, topic.getPartitions());
        assertEquals(replicationFactor, topic.getReplicationFactor());
      });
      assertTrue(crudLatch.await(10, TimeUnit.SECONDS));

    } catch (InterruptedException | KubernetesClientException e) {
      e.printStackTrace();
    }
  }

  @Test
  void testWatchConfigDeleteTopic() throws InterruptedException {

    String topicName = "test-toto";
    int partitions = 20;
    int replicationFactor = 3;
    String propsString = "partitions=20,compression.type=producer,retention.ms=3600000,replication-factor=3";
    Map<String, String> data = PropertyUtil.stringToMap(propsString);

    ObjectMeta metadata = new ObjectMeta();

    metadata.setName(topicName);
    Map<String, String> labels = Collections.singletonMap("config", "kafka-topic");
    metadata.setLabels(labels);
    ConfigMap cm3 = new ConfigMap("v1", data, "ConfigMap", metadata);
    cm3.setData(data);

    AppConfig appConfig = new AppConfig();
    Map<String, String> standardLabels = Collections.singletonMap("config", "kafka-topic");
    appConfig.setStandardLabels(standardLabels);
    KubernetesClient client = server.getClient();

    server.expect()
      .withPath("/api/v1/namespaces/test/configmaps?labelSelector=config%3Dkafka-topic&watch=true")
      .andUpgradeToWebSocket().open().waitFor(500).andEmit(new WatchEventBuilder().withConfigMapObject(cm3).withType("DELETED").build()).done().once();
    try (ConfigMapWatcher configMapWatcher = new ConfigMapWatcher(client, appConfig)) {
      configMapWatcher.watch();
      configMapWatcher.setOnDeleteListener((topic) -> {
        crudLatch.countDown();
        assertNotNull(topic);
      });
      assertTrue(crudLatch.await(10, TimeUnit.SECONDS));

    } catch (InterruptedException | KubernetesClientException e) {
      e.printStackTrace();
    }
  }

  @Test
  void testWatchConfigMapErrorTopic() throws InterruptedException, KubernetesClientException {

    String topicName = "test-toto";
    int partitions = 20;
    int replicationFactor = 3;
    String propsString = "partitions=20,compression.type=producer,retention.ms=3600000,replication-factor=3";
    Map<String, String> data = PropertyUtil.stringToMap(propsString);

    ObjectMeta metadata = new ObjectMeta();

    metadata.setName(topicName);
    Map<String, String> labels = Collections.singletonMap("config", "kafka-topic");
    metadata.setLabels(labels);
    ConfigMap cm3 = new ConfigMap("v1", data, "ConfigMap", metadata);
    cm3.setData(data);

    AppConfig appConfig = new AppConfig();
    Map<String, String> standardLabels = Collections.singletonMap("config", "kafka-topic");
    appConfig.setStandardLabels(standardLabels);
    KubernetesClient client = server.getClient();

    server.expect()
      .withPath(path)
      .andUpgradeToWebSocket().open().waitFor(500).andEmit(new WatchEventBuilder().withConfigMapObject(cm3).withType("ERROR").build()).done().once();
    try (ConfigMapWatcher configMapWatcher = new ConfigMapWatcher(client, appConfig)) {
      configMapWatcher.watch();
      crudLatch.countDown();
      assertTrue(crudLatch.await(10, TimeUnit.SECONDS));
    } catch (InterruptedException | KubernetesClientException e) {
      e.printStackTrace();
    }
  }

  @Test
  void testLabels() {
    String topicName = "test-toto";
    int partitions = 20;
    int replicationFactor = 3;
    String propsString = "partitions=20,compression.type=producer,retention.ms=3600000,replication-factor=3";
    Map<String, String> data = PropertyUtil.stringToMap(propsString);

    ObjectMeta metadata = new ObjectMeta();

    metadata.setName(topicName);
    Map<String, String> labels = Collections.singletonMap("config", "kafka-topic");
    metadata.setLabels(labels);
    ConfigMap cm3 = new ConfigMap("v1", data, "ConfigMap", metadata);
    cm3.setData(data);

    AppConfig appConfig = new AppConfig();
    Map<String, String> standardLabels = Collections.singletonMap("config", "kafka-topic");
    appConfig.setStandardLabels(standardLabels);
    KubernetesClient client = server.getClient();

    server.expect().withPath("/api/v1/namespaces/test/configmaps?labelSelector=config%3Dkafka-topic").andReturn(200, cm3).once();
    try (ConfigMapWatcher ConfigMapWatcher = new ConfigMapWatcher(client, appConfig)) {
      // Act
      ConfigMapWatcher.listTopics();
    }
  }
}


