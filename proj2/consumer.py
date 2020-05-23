import sys
from datetime import datetime
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from cassandra.cluster import Cluster
from uuid import uuid1


def analysis(input_data, min_lat, max_lat, min_long, max_long, min_datetime, max_datetime, cassandra_session):
    # Ako nema podataka, ne radi se nikakva obrada
    if input_data.isEmpty():
        print("No input data.")
        print("")
        return

    # Svaki rekord se transformise u niz reci
    fixed_data = input_data.map(lambda x: x[1].split(','))
    # Vrsi se parsiranje svih reci tako da se dobiju odgovarajuce vrednosti
    parsed_data = fixed_data.map(lambda x: list(map(lambda y: float(y), x[:-1])) +
                                 [datetime.strptime(x[-1], "%Y-%m-%d %H:%M:%S")])

    # Vrsi se filtriranje uz pomoc prosledjenih argumenata
    filtered_data = parsed_data.filter(lambda x: min_lat <= x[6] <= max_lat and min_long <= x[5] <= max_long
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
        print("")
        return

    # Za svaki atribut, racunaju se minimalna i maksimalna vrednost,
    # kao i prosecna vrednost i standardna devijacija
    # (rezultati obrade se cuvaju preko Cassandra sesije,
    # i takodje se vrsi njihovo ispisivanje na konzoli)
    attributes = ["Ozone", "Particulate matter",
                  "Carbon monoxide", "Sulfur dioxide", "Nitrogen dioxide"]
    for index, attribute in enumerate(attributes):
        atr_min = filtered_data.map(lambda x: x[index]).min()
        atr_max = filtered_data.map(lambda x: x[index]).max()
        atr_mean = filtered_data.map(lambda x: x[index]).mean()
        atr_std = filtered_data.map(lambda x: x[index]).stdev()

        cassandra_session.execute("INSERT INTO ivangd.Analysis_info (substance, timestamp, elements, min, max, mean, std) VALUES (%s, toTimeStamp(now()), %s, %s, %s, %s, %s)", (
            attribute, count, atr_min, atr_max, atr_mean, atr_std))

        print(attribute + " range: [" +
              str(atr_min) + ", " + str(atr_max) + "].")
        print(attribute + " mean: " + str(atr_mean) + ".")
        print(attribute + " standard deviation: " + str(atr_std) + ".")

    print("")


# Main funkcija koja prima kao argumente adresu Kafka brokera,
# ime topika sa kog se vrsi preuzimanje podataka za obradu,
# minimalnu i maksimalnu latitudu (geografsku sirinu),
# minimalnu i maksimalnu longitudu (geografsku duzinu),
# kao i minimalni i maksimalni vremenski trenutak (datetime),
# koji sluze za filtriranje podataka
def main(bootstrap_server, topic_name, time_interval, min_lat, max_lat, min_long, max_long, min_date, max_date):
    # Kreiranje Cassandra sesije radi cuvanja rezultata obrade u bazi
    cassandra_cluster = Cluster()
    cassandra_session = cassandra_cluster.connect(
        'ivangd', wait_for_all_pools=True)

    # Inicijalizacija konfiguracije i konteksta izvrsenja aplikacije
    configuration = SparkConf().setAppName("BigDataProj2")
    context = SparkContext(conf=configuration)
    context.setLogLevel("ERROR")

    # Instanciranje streaming konteksta
    # (tako da se obrada izvrsava na svakih time_interval sekundi),
    # kao i stream-a uz zadat topik i Kafka broker
    streaming_context = StreamingContext(context, time_interval)
    stream = KafkaUtils.createDirectStream(streaming_context, [topic_name], {
        "metadata.broker.list": bootstrap_server})

    # Za svaki RDD, definise se funkcija koja vrsi obradu podataka
    # (uz odgovarajuce prosledjene parametre za filtriranje podataka)
    stream.foreachRDD(lambda input_data: analysis(
        input_data, min_lat, max_lat, min_long, max_long, min_date, max_date, cassandra_session))

    # Zapocinje se obrada stream-a
    streaming_context.start()
    streaming_context.awaitTermination()


# Sintaksa za pokretanje programa s komandne linije je:
# spark-submit consumer.py bootstrap_server topic_name time_interval min_lat max_lat min_long max_long min_date max_date
# Na primer:
# spark-submit consumer.py localhost:9092 Pollution 5 56.1 56.2 10.1 10.2 2014-08-15 2014-09-15
if __name__ == "__main__":
    if len(sys.argv) != 10:
        # Greska nastupa ukoliko sintaksa nije dobra, tj. nema tacno 9 parametara
        print("\nBad command syntax. The correct parameter syntax is:")
        print("bootstrap_server topic_name time_interval min_lat max_lat min_long max_long min_date max_date\n")
    else:
        good = True
        try:
            bootstrap_server = sys.argv[1]
            topic_name = sys.argv[2]
            time_interval = float(sys.argv[3])
            min_lat = float(sys.argv[4])
            max_lat = float(sys.argv[5])
            min_long = float(sys.argv[6])
            max_long = float(sys.argv[7])
            min_date = datetime.strptime(sys.argv[8], "%Y-%m-%d")
            max_date = datetime.strptime(sys.argv[9], "%Y-%m-%d")
        except ValueError:
            # Greska nastupa ukoliko ne mogu svi parametri da se parsiraju uspesno
            print("\nInvalid parameters.\n")
            good = False
        if good:
            # Pokretanje programa uz odgovarajuce parsirane parametre
            main(bootstrap_server, topic_name, time_interval,
                 min_lat, max_lat, min_long, max_long, min_date, max_date)
