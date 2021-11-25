package ch.ethz.harvester.gisaid;


import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EthzParser {

    private final Pattern pattern = Pattern.compile(".*ETHZ-([0-9]+)/.*");

    public MaybeResult<Boolean> isOurs(Sequence sequence) {
        return new MaybeResult<>(sequence.getStrain().contains("-ETHZ-"));
    }


    public MaybeResult<Integer> parseEthid(Sequence sequence) {
        Matcher matcher = pattern.matcher(sequence.getStrain());
        try {
            if (matcher.find()) {
                int ethid = Integer.parseInt(matcher.group(1));
                return new MaybeResult<>(ethid, true, null);
            }
        } catch (NumberFormatException ignored) { }
        return new MaybeResult<>(null, false, new WeirdEntryReport(
                sequence.getGisaidEpiIsl(), "ch.ethz.harvester.gisaid.EthzParser::parseEthid",
                "The ETHID cannot be parsed from the strain name: " + sequence.getStrain()
        ));
    }
}
