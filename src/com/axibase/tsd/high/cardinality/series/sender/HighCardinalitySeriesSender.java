package com.axibase.tsd.high.cardinality.series.sender;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Logger;

/**
 * @author Igor Shmagrinskiy
 */
public class HighCardinalitySeriesSender {
    private static Logger logger = Logger.getLogger("com.wombat.nose");

    private static class TCPSender {
        private String url;
        private Integer port;
        private StringBuilder command;

        TCPSender(String url, Integer port) {
            this.url = url;
            this.port = port;
            command = new StringBuilder("debug ");
        }

        void setCommand(String command) {
            this.command = new StringBuilder(command);
        }

        public void appendCommand(String commandPart) {
            command.append(commandPart);
        }

        boolean sendDebugMode() throws IOException {
            Socket socket = new Socket(url, port);
            DataOutputStream requestStream = new DataOutputStream(socket.getOutputStream());
            BufferedReader responseStream = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            requestStream.writeBytes(command.insert(0, "debug ").append('\n').toString());
            String response = responseStream.readLine();
            socket.close();
            if (response == null) {
                return false;
            }
            return response.equals("ok");
        }

        public boolean sendDebugMode(long sleepDuration) throws IOException, InterruptedException {
            Socket socket = new Socket(url, port);
            DataOutputStream requestStream = new DataOutputStream(socket.getOutputStream());
            BufferedReader responseStream = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            requestStream.writeBytes(command.insert(0, "debug ").append('\n').toString());
            String response = responseStream.readLine();
            Thread.sleep(sleepDuration);
            socket.close();
            return response.equals("ok");
        }

        public void send() throws IOException {
            Socket socket = new Socket(url, port);
            DataOutputStream requestStream = new DataOutputStream(socket.getOutputStream());
            requestStream.writeBytes(command.append('\n').toString());
            requestStream.close();
            socket.close();
        }

        public void send(String command, long sleepDuration) throws IOException, InterruptedException {
            logger.fine(String.format(" > =====TCP=====\n > Sending via tcp://%s:%d\n > %s", url, port, command));
            Socket socket = new Socket(url, port);
            DataOutputStream requestStream = new DataOutputStream(socket.getOutputStream());
            requestStream.writeBytes(command + '\n');
            requestStream.close();
            Thread.sleep(sleepDuration);
            socket.close();
        }

        public void send(String command) throws IOException, InterruptedException {
            send(command, 0);
        }

        public void sendCheck(String command) throws IOException, InterruptedException {
            logger.fine(String.format(" > =====TCP=====\n > Sending via tcp://%s:%d\n > %s", url, port, command));
            setCommand(command);
            boolean successed = sendDebugMode();
            if (!successed)
                throw new IOException("Fail to check inserted command");
        }

        public void sendCheck(String command, long sleepDuration) throws IOException, InterruptedException {
            setCommand(command);
            boolean successed = sendDebugMode(sleepDuration);
            if (!successed)
                throw new IOException("Fail to check inserted command");
        }
    }

    public static class Util {
        public static String ISOFormat(Date date, boolean withMillis, String timeZoneName) {
            String pattern = (withMillis) ? "yyyy-MM-dd'T'HH:mm:ss.SSSXXX" : "yyyy-MM-dd'T'HH:mm:ssXXX";
            SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);
            dateFormat.setTimeZone(TimeZone.getTimeZone(timeZoneName));
            return dateFormat.format(date);
        }

        public static String ISOFormat(Long time) {
            return ISOFormat(new Date(time), false, "UTC");
        }

        public static Date parseDate(String date) {
            return javax.xml.bind.DatatypeConverter.parseDateTime(date).getTime();
        }
    }

    public static class Sample {
        private String d;
        private Long t;
        private BigDecimal v;

        public Sample() {
        }

        public Sample(long t, String v) {
            this.t = t;
            this.v = new BigDecimal(String.valueOf(v));
        }

        public Sample(String d, int v) {
            this.d = d;
            this.v = new BigDecimal(String.valueOf(v));
        }

        public Sample(Long t, BigDecimal v) {
            this.t = t;
            this.v = v;
        }

        public Sample(String d, BigDecimal v) {
            this.d = d;
            this.v = v;
        }

        public Sample(String d, String v) {
            this.d = d;
            this.v = new BigDecimal(String.valueOf(v));
        }


        public Long getT() {
            return t;
        }

        public String getD() {
            return d;
        }

        public BigDecimal getV() {
            return v;
        }

        public void setD(String d) {
            this.d = d;
        }

        public void setV(BigDecimal v) {
            this.v = v;
        }

        @Override
        public String toString() {
            return "Sample{" +
                    "d='" + d + '\'' +
                    ", t=" + t +
                    ", v=" + v +
                    '}';
        }
    }

    private static List<String> networkSeriesCommand(String metric, String entity, Map<String, String> tags, List<Sample> data) {
        List<String> seriesCommandList = new ArrayList<>();
        StringBuilder stringBuilder = new StringBuilder();
        if (metric == null) {
            throw new IllegalStateException("Empty entity");
        }

        if (entity == null) {
            throw new IllegalStateException("Empty entity");
        }

        for (Sample sample : data) {
            stringBuilder.setLength(0);
            stringBuilder.append("series");
            stringBuilder
                    .append(" e:")
                    .append(entity);

            stringBuilder
                    .append(" m:")
                    .append(metric)
                    .append('=')
                    .append(sample.getV().toString());

            stringBuilder
                    .append(" d:")
                    .append(sample.getD());
            for (String key : tags.keySet()) {
                stringBuilder
                        .append(" t:")
                        .append(key)
                        .append('=')
                        .append(tags.get(key));

            }
            seriesCommandList.add(stringBuilder.toString());
        }
        return seriesCommandList;
    }

    private static Map<String, String> generateTags(String timestamp, String value, Integer capacity) {
        Map<String, String> tags = new HashMap<>();
        tags.put("tag-timestamp", timestamp);
        tags.put("tag-value", value);
        int i = 0;
        while (tags.size() < capacity) {
            i++;
            tags.put("tag-" + i, "val-" + i);
        }
        return tags;
    }

    private static List<String> generateCommands(String entity, String metric, Long deltaTime, Long startTime, Integer recordsCount, Integer tagsCapacity) {
        Long time = startTime;
        List<String> commands = new ArrayList<>();
        for (int j = 0; j < recordsCount; ) {
            String stringValue = Double.toString(Math.sin(time));
            String dateTime = Util.ISOFormat(time);
            List<Sample> data = Collections.singletonList(new Sample(dateTime, stringValue));
            Map<String, String> tags = generateTags(dateTime, stringValue, tagsCapacity);
            time += deltaTime;
            List<String> currentCommands = networkSeriesCommand(metric, entity, tags, data);
            j += currentCommands.size();
            commands.addAll(currentCommands);
        }
        return commands;
    }

    private static void sendCommands(String url, Integer port, List<String> list, Integer portion) throws IOException, InterruptedException {
        TCPSender tcpSender = new TCPSender(url, port);

        StringBuilder stringBuilder = new StringBuilder();

        for (int i = 0; i < list.size(); ) {
            stringBuilder.setLength(0);

            for (int j = 0; j < portion && i < list.size(); j++) {
                stringBuilder
                        .append(list.get(i))
                        .append('\n');
                i++;
            }

            tcpSender.sendCheck(stringBuilder.toString());
            logger.info(String.format("Commands sent: %d", i));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        final String ATSD_URL = "localhost";//atsd hostname
        final Integer ATSD_TCP_PORT = 8081;//atsd tcp port
        final String DEFAULT_ENTITY_NAME = "high-cardinality-entity";//entity name
        final String DEFAULT_METRIC_NAME = "high-cardinality-metric";//metric name
        final String START_TIME = "2016-08-09T00:00:00.000Z";//start series time
        final Long DELTA_TIME = 15000L;//delta time between series
        final Integer RECORDS_COUNT = 50000;//series to send
        final Integer TAGS_CAPACITY = 50;//tags amount in series
        final Integer SENT_PORTITON = 10;//number of commnands in one sending

        List<String> commands = generateCommands(
                DEFAULT_ENTITY_NAME,
                DEFAULT_METRIC_NAME,
                DELTA_TIME,
                Util.parseDate(START_TIME).getTime(),
                RECORDS_COUNT,
                TAGS_CAPACITY
        );

        sendCommands(ATSD_URL, ATSD_TCP_PORT, commands, SENT_PORTITON);
    }
}