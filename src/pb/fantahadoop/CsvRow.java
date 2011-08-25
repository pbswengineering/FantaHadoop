package pb.fantahadoop;

public class CsvRow {

    public int presenze;
    public int codice;
    public String nome;
    public String squadra;
    public String ruolo;
    public double voto;
    public int golFatti;
    public int golSubiti;
    public int golVittoria;
    public int golPareggio;
    public int assist;
    public int ammonizioni;
    public int espulsioni;
    public int rigParati;
    public int rigFalliti;
    public int autogol;

    private static int parseInt(String s) {
        return s.length() == 0 ? 0 : Integer.parseInt(s);
    }

    private static double parseDouble(String s) {
        return s.length() == 0 ? 0 : Double.parseDouble(s);
    }

    public CsvRow(String rowText) {
        String row[] = {};

        row = rowText.replace("\"", "").split("\\s*,\\s*", -1);
        presenze = 1;
        codice = parseInt(row[0]);
        nome = row[1];
        squadra = row[2];
        ruolo = row[3];
        voto = parseDouble(row[4]);
        golFatti = parseInt(row[5]);
        golSubiti = parseInt(row[6]);
        golVittoria = parseInt(row[7]);
        golPareggio = parseInt(row[8]);
        assist = parseInt(row[9]);
        ammonizioni = parseInt(row[10]);
        espulsioni = parseInt(row[11]);
        rigParati = parseInt(row[12]);
        rigFalliti = parseInt(row[13]);
        autogol = parseInt(row[14]);

    }

    public double calcolaVoto() {
        return voto
                + golFatti * 3
                - golSubiti
                + assist
                - ammonizioni * 0.5
                - espulsioni
                + rigParati * 3
                - rigFalliti * 3
                - autogol;
    }
}
