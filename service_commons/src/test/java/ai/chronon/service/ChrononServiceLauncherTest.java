package ai.chronon.service;

import ai.chronon.online.metrics.OtelMetricsReporter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ChrononServiceLauncherTest {

    private static final String CHRONON_RESOURCES_PROP = OtelMetricsReporter.MetricsExporterResourceKey();

    private String savedChrononProp;

    @BeforeEach
    void saveSysProps() {
        savedChrononProp = System.getProperty(CHRONON_RESOURCES_PROP);
        System.clearProperty(CHRONON_RESOURCES_PROP);
    }

    @AfterEach
    void restoreSysProps() {
        if (savedChrononProp == null) {
            System.clearProperty(CHRONON_RESOURCES_PROP);
        } else {
            System.setProperty(CHRONON_RESOURCES_PROP, savedChrononProp);
        }
    }

    private Function<String, String> envOf(Map<String, String> env) {
        return env::get;
    }

    // Mirrors how Micrometer's OtlpConfig parses the resourceAttributes string into a LinkedHashMap
    // where the last value for a duplicate key wins. Asserting against this parsed view keeps the
    // precedence checks meaningful even if the raw string layout changes.
    private Map<String, String> parseAsMicrometerWould(String resourceAttributes) {
        Map<String, String> parsed = new LinkedHashMap<>();
        for (String entry : resourceAttributes.split(",")) {
            String[] kv = entry.split("=", 2);
            if (kv.length == 2) {
                parsed.put(kv[0].trim(), kv[1].trim());
            }
        }
        return parsed;
    }

    @Test
    void onlyDefaultServiceNameWhenNothingElseConfigured() {
        String result = ChrononServiceLauncher.buildOtlpResourceAttributes(
                "ai.chronon", envOf(new HashMap<>()));

        assertEquals("service.name=ai.chronon", result);
    }

    @Test
    void allFourSourcesCombineWithCorrectPrecedence() {
        // Both OTEL_RESOURCE_ATTRIBUTES and the chronon sysprop set deployment.environment.name —
        // the sysprop is appended later so it must win. OTEL_SERVICE_NAME is appended last and
        // must beat the default service.name.
        Map<String, String> env = new HashMap<>();
        env.put("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment.name=staging,team=ml");
        env.put("OTEL_SERVICE_NAME", "feature-service-prod");
        System.setProperty(CHRONON_RESOURCES_PROP, "deployment.environment.name=prod,region=us-east-1");

        Map<String, String> parsed = parseAsMicrometerWould(
                ChrononServiceLauncher.buildOtlpResourceAttributes("ai.chronon", envOf(env)));

        assertEquals("feature-service-prod", parsed.get("service.name"));
        assertEquals("prod", parsed.get("deployment.environment.name"));
        assertEquals("ml", parsed.get("team"));
        assertEquals("us-east-1", parsed.get("region"));
    }

    @Test
    void blankInputsAreIgnoredWithoutTrailingDelimiters() {
        // Guards against producing strings like "service.name=ai.chronon,," that would parse
        // into spurious empty entries on the receiving side.
        Map<String, String> env = new HashMap<>();
        env.put("OTEL_RESOURCE_ATTRIBUTES", "   ");
        env.put("OTEL_SERVICE_NAME", "");
        System.setProperty(CHRONON_RESOURCES_PROP, "");

        String result = ChrononServiceLauncher.buildOtlpResourceAttributes("ai.chronon", envOf(env));

        assertEquals("service.name=ai.chronon", result);
    }
}
