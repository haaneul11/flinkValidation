package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;


import java.util.Properties;
import java.util.concurrent.CompletableFuture;

public class ValidationJob {

    public static void main(String[] args) throws Exception {
        // 기본 환경설정
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(); // 로컬에서 작업하겠다는 의미
        env.enableCheckpointing(5000);  // 5초
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink-checkpoints");

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("checkout-requested")
                .setGroupId("validation_checker")
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .build();

        // Kafka Sink 설정 (Valid-checkoutRequest)
        KafkaSink<String> kafkaValidSink = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("Valid-checkoutRequest")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        // Kafka Sink 설정 (Invalid-checkoutRequest)
        KafkaSink<String> kafkaInvalidSink = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("Invalid-checkoutRequest")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        // Kafka consumer
        Properties kafkaConsumerConfig = new Properties();
        kafkaConsumerConfig.setProperty("bootstrap.servers", "localhost:9092");
        kafkaConsumerConfig.setProperty("group.id", "validation_checker");

        // kafka producer - checkoutValid (유효한 것)
        Properties kafkaCheckoutValidProducer = new Properties();
        kafkaCheckoutValidProducer.setProperty("bootstrap.servers", "localhost:9092");
        kafkaCheckoutValidProducer.setProperty("transaction.timeout.ms", "60000");

        // kafka producer - checkoutInvalid (유효하지 않은 것)
        Properties kafkaCheckoutInvalidProducer = new Properties();
        kafkaCheckoutInvalidProducer.setProperty("bootstrap.servers", "localhost:9092");
        kafkaCheckoutInvalidProducer.setProperty("transaction.timeout.ms", "60000");

        // sideout 정의 🌟
        final OutputTag<String> invalidTag = new  OutputTag<String>("InvalidRequest") {};
        SingleOutputStreamOperator<String> validStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),"kafka Source")
                .process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
                        ObjectMapper objectMapper = new ObjectMapper();
                        try {
                            CheckoutEvent checkoutEvent = objectMapper.readValue(s, CheckoutEvent.class);

                            CompletableFuture<Boolean> userValidation = validateUserId(checkoutEvent.getUserId());
                            CompletableFuture<Boolean> shippingInfoValidation = validateShippingInfo(checkoutEvent.getShippingMasterId());
                            CompletableFuture<Boolean> productValidation = validateProductId(checkoutEvent.getProductId(), checkoutEvent.getQuantity());
                            CompletableFuture<Boolean> paymentIdValidation = validatePaymentId(checkoutEvent.getPaymentId());

                            // validation result
                            boolean isUserValid = userValidation.get();
                            boolean isShippingInfoValid = shippingInfoValidation.get();
                            boolean isProductIdValid = productValidation.get();
                            boolean isPaymentIdValid = paymentIdValidation.get();

                            checkoutEvent.setUserValid(isUserValid);
                            checkoutEvent.setShippingInfoValid(isShippingInfoValid);
                            checkoutEvent.setProductValid(isProductIdValid);
                            checkoutEvent.setPaymentValid(isPaymentIdValid);

                            if (isUserValid && isShippingInfoValid && isProductIdValid && isPaymentIdValid) {
                                collector.collect(objectMapper.writeValueAsString(checkoutEvent));
                            } else {
                                ValidationFailure validationFailure = new ValidationFailure(
                                        checkoutEvent.getId(),
                                        isUserValid,
                                        isShippingInfoValid,
                                        isProductIdValid,
                                        isPaymentIdValid
                                );
                                context.output(invalidTag, objectMapper.writeValueAsString(validationFailure));
                            }

                        } catch (Exception e) {
                            ValidationFailure validationFailure = new ValidationFailure(
                                    -1L, false, false, false, false
                            );
                            context.output(invalidTag, "Error processing checkout request: " + e.getMessage());
                        }
                    }
                });

        // produce
        DataStream<String> failureStream = validStream.getSideOutput(invalidTag);
        validStream.sinkTo(kafkaValidSink); // 어디 타겟으로 갈지 정해주는 것
        failureStream.sinkTo(kafkaInvalidSink); // 어디 타겟으로 갈지 정해주는 것
        env.execute("checkout-validation-job");


    }

    private static CompletableFuture<Boolean> validateUserId(String userId) {
        // 추가 사용자 검증로직 (blacklist(블랙리스트 검증), seller와의 연관성 찾기, traffic(이상 트래픽 검증))
        return CompletableFuture.supplyAsync(() -> userId != null && !userId.trim().isEmpty());
    }

    private static CompletableFuture<Boolean> validateShippingInfo(String shippingMasterId) {
        // 추가 로직
        return CompletableFuture.supplyAsync(() -> shippingMasterId != null && !shippingMasterId.trim().isEmpty());
    }

    private static CompletableFuture<Boolean> validateProductId(String productId, Integer orderQty) {
        // 재고 확인 로직
//        return CompletableFuture.supplyAsync(() -> productId != null && !productId.trim().isEmpty() && quantity > 0);

        return CompletableFuture.supplyAsync(() -> {
            String inventoryValidationCheckUrl = "localhost:8802/inventory/products/" + productId + "/orderYn/?orderQty=" + orderQty;

            try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
                HttpGet request = new HttpGet(inventoryValidationCheckUrl);
                try (CloseableHttpResponse response = httpClient.execute(request)) {
                    int statusCode = response.getStatusLine().getStatusCode();

                    if(statusCode == 200) {
                        String responseBody = EntityUtils.toString(response.getEntity());
                        return Boolean.parseBoolean(responseBody);
                    }
                    else {
                        System.out.println("API Error : " + statusCode + " : " + EntityUtils.toString(response.getEntity()));
                        return Boolean.FALSE;
                    }
                }

            }
            catch(Exception e ){
                System.out.println("Error to get response from inventory APIs : " + e.getMessage());
                return Boolean.FALSE;
            }
        });
    }


    private static CompletableFuture<Boolean> validatePaymentId(String paymentId) {
        // 카드의 유효성 확인
        return CompletableFuture.supplyAsync(() -> paymentId != null && paymentId.trim().startsWith("pm_"));
    }

}
