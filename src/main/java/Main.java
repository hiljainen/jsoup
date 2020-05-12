import lombok.SneakyThrows;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Main {
    private static final String url = "https://ru.wikipedia.org/wiki/";
    private static final String localUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) " +
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36";
    private static final String localPath = "src/main/sourceFiles";
    private static final String query = URLEncoder.
            encode("Список_станций_Московского_метрополитена", StandardCharsets.UTF_8);
    private static final String jsonFileOut = "moscow_underground_stations.json";
    private static final String tableSelector = "standard sortable";
    private static final JSONObject listOfLines = new JSONObject();
    private static final JSONArray listOfConnections = new JSONArray();
    private static final JSONObject resultJsonObject = new JSONObject();

    @FunctionalInterface
    private interface BiFunctionThrowable<T, U, R> {
        R apply(T t, U u) throws IOException;
    }

    @FunctionalInterface
    private interface TriConsumerThrowable<T, U, V> {
        void accept(T t, U u, V v) throws IOException;
    }

    @SneakyThrows(IOException.class)
    public static void main(String[] args) {
        final Document page = getJsoupDocument.apply((url + query), localUserAgent);
        page.getElementsByClass(tableSelector).forEach(parseTableToJson);
        resultJsonObject.put("stations", listOfLines);
        resultJsonObject.put("connections", removeDuplicates.apply(listOfConnections));
        Files.createDirectories(Paths.get(localPath).toAbsolutePath());
        writeJsonToFile.accept(resultJsonObject, localPath, jsonFileOut);
        writeJsonToFile.accept(resultJsonObject, localPath, jsonFileOut);
        JSONObject filePathJsonFile = new JSONObject();
        filePathJsonFile.put("filePath", Paths.get(localPath +"/" + jsonFileOut).toAbsolutePath().toString());
        writeJsonToFile.accept(filePathJsonFile, localPath, "file_path_to_json.json");
    }

    private static final Function<JSONArray, JSONArray> removeDuplicates = jsonArrayIn -> {
        Set<JSONArray> jsonArraySet = new HashSet<>();
        JSONArray jsonArrayOut = new JSONArray();
        jsonArrayIn.forEach(e -> jsonArraySet.add(((JSONArray) e)));
        jsonArrayOut.addAll(jsonArraySet);
        return jsonArrayOut;
    };

    private static final BiFunctionThrowable<String, String, Document> getJsoupDocument = (urlWithQuery, userAgent) ->
            Jsoup.connect(urlWithQuery).userAgent(userAgent).get();

    private static final BiConsumer<String, String> addStationAndLineToList = (line, station) -> {
        JSONArray jsonArray = new JSONArray();
        if (listOfLines.containsKey(line)) {
            jsonArray = (JSONArray) listOfLines.get(line);
        }
        jsonArray.add(station);
        listOfLines.put(line, jsonArray);
    };
    private static final Function<String, String> removeLeadingZero = element -> element.charAt(0) == '0'
            ? element.replaceFirst("0", "") : element;

    private static final Function<Element, JSONArray> parseLineConnections = lineConnection -> {
        final JSONArray connection = new JSONArray();
        final String[] numberOfConnectedLines = lineConnection.getElementsByClass("sortkey").text().split("\\s+");
        final AtomicInteger i = new AtomicInteger();

        for (Element link : lineConnection.getElementsByTag("a")) {
            final JSONObject connectionObject = new JSONObject();
            String line = removeLeadingZero.apply(numberOfConnectedLines[i.getAndIncrement()]);
            try {
                Document page = getJsoupDocument.apply(link.attr("abs:href"), localUserAgent);
                String station = page.getElementById("firstHeading").text();
                if (station.contains("("))
                    station = station.substring(0, station.indexOf("(")).trim();
                connectionObject.put("line", line);
                connectionObject.put("station", station);
            } catch (IOException e) {
                String string = "^.+станцию\\s(.+)\\sМоск.+$";
                Pattern pattern = Pattern.compile(string);
                Matcher matcher = pattern.matcher(link.attr("title"));
                connectionObject.put("line", line);
                if (matcher.find()) {
                    connectionObject.put("station", matcher.group(1));
                } else {
                    connectionObject.put("station", "Станция не определена");
                    System.err.println(link + " - Станция не определена");
                }
                System.err.println("Невозможно установить соединение со страницей станции." +
                        " Название станции вязто из таблицы начальной страницы");
                e.printStackTrace();
            }
            connection.add(connectionObject);
        }
        return connection;
    };

    private static final BiPredicate<Object[], Integer> checkLength = (array, length) -> array.length == length;

    private static final Consumer<Element> parseTableToJson = table -> {
        final String lineNumberSelector = "td:nth-of-type(1)";
        final String stationNameSelector = "td:nth-of-type(2)";
        final String lineConnectionsSelector = "td:nth-of-type(4)";
        table.getElementsByTag("tr").forEach(tableRow -> tableRow.select(lineConnectionsSelector).forEach(lineConnection -> {
            final String stationName;
            final String[] numbers;
            final String line1;
            final String line2;
            final JSONArray connection;
            final JSONObject connectionObject1 = new JSONObject();
            final JSONObject connectionObject2 = new JSONObject();
            if (!tableRow.select(lineNumberSelector).select("td > span").text().equals("")
                    && !tableRow.select(stationNameSelector).select("a[href]").text().equals("")) {
                numbers = tableRow.select(lineNumberSelector).select("td > span").text().split("\\s+");
                stationName = tableRow.select(stationNameSelector).select("a[href]").first().text();
                line1 = removeLeadingZero.apply(numbers[0]);
                addStationAndLineToList.accept(line1, stationName);
                connectionObject1.put("line", line1);
                connectionObject1.put("station", stationName);
                if (checkLength.test(numbers, 3)) {
                    line2 = removeLeadingZero.apply(numbers[1]);
                    addStationAndLineToList.accept(line2, stationName);
                    connectionObject2.put("line", line2);
                    connectionObject2.put("station", stationName);
                }
                connection = parseLineConnections.apply(lineConnection);
                if (connection.size() != 0) {
                    connection.add(0, connectionObject1);
                    if (checkLength.test(numbers, 3)) {
                        connection.add(1, connectionObject2);
                    }
                    JSONArray jsonArray = new JSONArray();
                    jsonArray.addAll((Collection) connection.stream()
                            .sorted(Comparator.comparing(e -> ((JSONObject) e).get("line").toString())).collect(Collectors.toList()));
                    listOfConnections.add(jsonArray);
                }
            }
        }));
    };

    private static final TriConsumerThrowable<JSONObject, String, String> writeJsonToFile = (jsonObject, filePath, fileName) -> {
        Path path = Paths.get(filePath + "/" + fileName).toAbsolutePath();
        if (!Files.exists(path))
            Files.createFile(path);
        Files.write(path, jsonObject.toJSONString().getBytes());
    };
}