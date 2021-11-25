package ch.ethz.harvester.gisaid;

import java.util.HashMap;
import java.util.Map;


public class GeographicalDivisionCleaner {

    private static final Map<String, Map<String, String>> countries = new HashMap<>() {{
       put("CHE", new HashMap<>() {{
           put("Aargau", "Aargau");
           put("Appenzell Ausserrhoden", "Appenzell Ausserrhoden");
           put("Appenzell-Ausserrhoden", "Appenzell Ausserrhoden");
           put("Argovie", "Aargau");
           put("BE", "Bern");
           put("Bale", "Basel-Stadt");
           put("Basel", "Basel-Stadt");
           put("Basel-City", "Basel-Stadt");
           put("Basel-Land", "Basel-Landschaft");
           put("Basel-Landschaft", "Basel-Landschaft");
           put("Basel-Stadt", "Basel-Stadt");
           put("Bern", "Bern");
           put("Berne", "Bern");
           put("Freibrug", "Fribourg");
           put("Freiburg", "Fribourg");
           put("Fribourg", "Fribourg");
           put("Geneva", "Geneva");
           put("Geneve", "Geneva");
           put("Genf", "Geneva");
           put("Glarus", "Glarus");
           put("Graubuenden", "Graubünden");
           put("Graubunden", "Graubünden");
           put("GraubÃ¼nden", "Graubünden");
           put("Graubünden", "Graubünden");
           put("Graub�nden", "Graubünden");
           put("Grisons", "Graubünden");
           put("JU", "Jura");
           put("Jura", "Jura");
           put("Lucerne", "Lucerne");
           put("Luzern", "Lucerne");
           put("Na", null);
           put("Neuchatel", "Neuchâtel");
           put("NeuchÃ¢tel", "Neuchâtel");
           put("Neuchâtel", "Neuchâtel");
           put("Neuenburg", "Neuchâtel");
           put("Nidwalden", "Nidwalden");
           put("Obwald", "Obwalden");
           put("Obwalden", "Obwalden");
           put("Saint-Gall", "St Gallen");
           put("Saint-Gallen", "St Gallen");
           put("Sankt Gallen", "St Gallen");
           put("Schaffhausen", "Schaffhausen");
           put("Schaffhouse", "Schaffhausen");
           put("Schwyz", "Schwyz");
           put("Solothurn", "Solothurn");
           put("St Gallen", "St Gallen");
           put("St. Gallen", "St Gallen");
           put("St.Gallen", "St Gallen");
           put("Stankt Gallen", "St Gallen");
           put("Tessin", "Ticino");
           put("Thurgau", "Thurgau");
           put("Ticino", "Ticino");
           put("Turgovia", "Thurgau");
           put("Uri", "Uri");
           put("VALAIS", "Valais");
           put("Valais", "Valais");
           put("Vaud", "Vaud");
           put("Waadt", "Vaud");
           put("Wallis", "Valais");
           put("Zaerich", "Zurich");
           put("Zoerich", "Zurich");
           put("Zuerich", "Zurich");
           put("Zug", "Zug");
           put("Zurich", "Zurich");
           put("Zürich", "Zurich");
       }});
       put("GBR", new HashMap<>() {{
           put("London", "England");
       }});
    }};

    public static String resolve(String country, String division) {
        if (division == null) {
            return null;
        }
        if (division.equalsIgnoreCase("unknown")) {
            return null;
        }
        if (country == null) {
            return division;
        }
        Map<String, String> divisions = countries.get(country);
        if (divisions == null) {
            return division;
        }
        String clean = divisions.get(division);
        if (clean == null) {
            return division;
        }
        return clean;
    }

}
