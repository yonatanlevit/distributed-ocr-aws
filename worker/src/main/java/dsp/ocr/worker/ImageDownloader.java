package dsp.ocr.worker;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Downloads an image with strict limits, so a dead or stalling server becomes a quick error
 * result instead of holding the Worker until the heartbeat cap.
 */
final class ImageDownloader {
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);
    /** Time allowed until the response headers arrive. */
    private static final Duration RESPONSE_TIMEOUT = Duration.ofSeconds(30);
    private static final int MAX_BYTES = 20 * 1024 * 1024;

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(CONNECT_TIMEOUT)
            .followRedirects(HttpClient.Redirect.NORMAL) // follows redirects, except https → http
            .build();

    byte[] download(String url) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(url.strip()))
                .timeout(RESPONSE_TIMEOUT)
                // Some image hosts reject requests without a browser-like User-Agent.
                .header("User-Agent", "Mozilla/5.0 (compatible; ocr-worker)")
                .GET()
                .build();
        HttpResponse<InputStream> response = http.send(request, HttpResponse.BodyHandlers.ofInputStream());
        try (InputStream body = response.body()) {
            if (response.statusCode() != 200) {
                throw new IOException("HTTP " + response.statusCode());
            }
            byte[] data = body.readNBytes(MAX_BYTES + 1);
            if (data.length > MAX_BYTES) {
                throw new IOException("image larger than " + MAX_BYTES / (1024 * 1024) + " MB");
            }
            return data;
        }
    }
}
