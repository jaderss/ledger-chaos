package dev.jaderss.chaos;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;
import java.util.regex.Pattern;

final class GoDuration {

    private static final Pattern PART = Pattern.compile("(\\d++(?:\\.\\d++)?+)(ms|s|m|h)");
    private static final Map<String, Long> NANOS =
            Map.of("ms", 1_000_000L, "s", 1_000_000_000L, "m", 60_000_000_000L, "h", 3_600_000_000_000L);

    private GoDuration() {}

    static Duration parse(String value) {
        var matcher = PART.matcher(value);
        var nanos = BigDecimal.ZERO;
        var end = 0;
        while (matcher.find() && matcher.start() == end) {
            nanos = nanos.add(
                    new BigDecimal(matcher.group(1)).multiply(BigDecimal.valueOf(NANOS.get(matcher.group(2)))));
            end = matcher.end();
        }
        if (end == 0 || end != value.length()) {
            throw new IllegalArgumentException("not a duration: \"%s\"".formatted(value));
        }
        return Duration.ofNanos(nanos.longValueExact());
    }

    static String format(Duration duration) {
        if (duration.isZero()) {
            return "0s";
        }
        if (duration.compareTo(Duration.ofSeconds(1)) < 0) {
            return duration.toMillis() + "ms";
        }
        var text = new StringBuilder();
        if (duration.toHours() > 0) {
            text.append(duration.toHours()).append('h');
        }
        if (duration.toMinutes() > 0) {
            text.append(duration.toMinutesPart()).append('m');
        }
        var seconds = BigDecimal.valueOf(duration.toSecondsPart())
                .add(BigDecimal.valueOf(duration.toMillisPart(), 3))
                .stripTrailingZeros();
        return text.append(seconds.toPlainString()).append('s').toString();
    }
}
