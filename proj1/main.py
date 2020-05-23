import sys
from pyspark import SparkConf, SparkContext
from datetime import datetime


# Main funkcija koja prima kao argumente putanju koja odredjuje fajl/fajlove za obradu,
# minimalnu i maksimalnu latitudu (geografsku sirinu),
# minimalnu i maksimalnu longitudu (geografsku duzinu),
# kao i minimalni i maksimalni vremenski trenutak (datetime),
# koji sluze za filtriranje podataka
def main(input_path, min_lat, max_lat, min_long, max_long, min_datetime, max_datetime):

    # Inicijalizacija konfiguracije i konteksta izvrsenja aplikacije
    configuration = SparkConf().setAppName("BigDataProj1")
    context = SparkContext(conf=configuration)
    context.setLogLevel("ERROR")

    # Ucitavaju se podaci iz odgovarajuce putanje,
    # zatim se vrsi razbijanje u odnosu na zarez, posto su u pitanju CSV fajlovi,
    # pa se odbacuju redovi koji predstavljaju header
    # i na kraju formira lista koja predstavlja torku
    # (timestamp se parsira u datetime objekat)
    input_data = context.textFile(input_path)
    input_data = input_data.map(lambda x: x.split(","))
    n = len(input_data.first())
    input_data = input_data.filter(lambda x: x[n - 1] != "timestamp")
    input_data = input_data.map(lambda x: list(map(lambda y: float(y), x[:-1])) +
                                          [datetime.strptime(x[-1], "%Y-%m-%d %H:%M:%S")]
                                )

    # Vrsi se filtriranje uz pomoc prosledjenih argumenata
    filtered_data = input_data.filter(lambda x: min_lat <= x[6] <= max_lat and min_long <= x[5] <= max_long
                                                and min_datetime <= x[7] <= max_datetime
                                      )
    # Dobijeni isfiltrirani podaci se kesiraju
    filtered_data.cache()

    # Broji se koliko ima ukupno elemenata i ispisuje
    count = filtered_data.count()
    print("Total number of elements: " + str(count) + ".")

    # Ukoliko ima 0 elemenata, tj. RDD je prazan, ne vrsi se nikakva dalja analiza
    if filtered_data.isEmpty():
        print("There are no elements so no further analysis can be made.")
        return

    # Za svaki atribut, racunaju se minimalna i maksimalna vrednost,
    # kao i prosecna vrednost i standardna devijacija
    attributes = ["Ozone", "Particulate matter", "Carbon monoxide", "Sulfur dioxide", "Nitrogen dioxide"]
    for index, attribute in enumerate(attributes):
        atr_min = filtered_data.map(lambda x: x[index]).min()
        atr_max = filtered_data.map(lambda x: x[index]).max()
        atr_mean = filtered_data.map(lambda x: x[index]).mean()
        atr_std = filtered_data.map(lambda x: x[index]).stdev()
        print(attribute + " range: [" + str(atr_min) + ", " + str(atr_max) + "].")
        print(attribute + " mean: " + str(atr_mean) + ".")
        print(attribute + " standard deviation: " + str(atr_std) + ".")


# Sintaksa za pokretanje programa s komandne linije je:
# spark-submit main.py input_path min_lat max_lat min_long max_long min_date max_date
# Na primer:
# spark-submit main.py hdfs://localhost:9000/pollution 56.1 56.2 10.1 10.2 2014-08-15 2014-09-15
if __name__ == "__main__":
    if len(sys.argv) != 8:
        # Greska nastupa ukoliko sintaksa nije dobra, tj. nema tacno 7 parametara
        print("\nBad command syntax. The correct parameter syntax is:")
        print("input_path min_lat max_lat min_long max_long min_date max_date\n")
    else:
        good = True
        try:
            input_path = sys.argv[1]
            min_lat = float(sys.argv[2])
            max_lat = float(sys.argv[3])
            min_long = float(sys.argv[4])
            max_long = float(sys.argv[5])
            min_date = datetime.strptime(sys.argv[6], "%Y-%m-%d")
            max_date = datetime.strptime(sys.argv[7], "%Y-%m-%d")
        except ValueError:
            # Greska nastupa ukoliko ne mogu svi parametri da se parsiraju uspesno
            print("\nInvalid parameters.\n")
            good = False
        if good:
            # Pokretanje programa uz odgovarajuce parsirane parametre
            main(input_path, min_lat, max_lat, min_long, max_long, min_date, max_date)