import sys
from glob import glob
from kafka import KafkaProducer


# Main funkcija koja prima kao argumente putanju koja odredjuje
# folder u kom se nalaze svi fajlovi za obradu
# (uzimaju se u obzir samo .csv fajlovi),
# adresu Kafka brokera, kao i ime topika
# na koji se vrsi objava od strane producer-a
def main(input_path, bootstrap_server, topic_name):

    # Instancira se Kafka Producer
    # (konstruktoru se prosledjuje adresa brokera,
    # kao i funkcija koja vrsi obicnu serijalizaciju stringa u niz bajtova)
    kafka_producer = KafkaProducer(bootstrap_servers=bootstrap_server,
                                   value_serializer=lambda x: x.encode("utf-8"), api_version=(0, 1, 0))

    # Vrsi se obrada svakog .csv fajla u datom folderu
    for file_path in glob(input_path + "/*.csv"):
        with open(file_path, 'r') as opened_file:
            # Svaka linija predstavlja neki rekord
            # (osim pocetne linije u svakom fajlu, koja je header)
            for line in opened_file:
                # Uklanjaju se nezeljeni prazni znaci
                info = line.strip()
                split_info = info.split(',')
                n = len(split_info)
                if n == 0:
                    # Eventualne prazne linije se ignorisu
                    continue
                # Ne uzimaju se u obzir header-i, kod kojih je zadnja rec zapravo "timestamp"
                if "timestamp" not in split_info[0].lower():
                    # Salje se odgovarajuca poruka na topik
                    kafka_producer.send(topic_name, value=info)
                    kafka_producer.flush()
                    # Stampa se poslata poruka na konzolu
                    print(info)


# Sintaksa za pokretanje programa s komandne linije je:
# python producer.py input_path bootstrap_server topic_name
# Na primer:
# python producer.py /home/aleksandra.stojnev/apps/ivan.damnjanovic/merged localhost:9092 Merged
if __name__ == "__main__":
    if len(sys.argv) != 4:
        # Greska nastupa ukoliko sintaksa nije dobra, tj. nema tacno 3 parametra
        print("\nBad command syntax. The correct parameter syntax is:")
        print("input_path bootstrap_server topic_name\n")
    else:
        input_path = sys.argv[1]
        bootstrap_server = sys.argv[2]
        topic_name = sys.argv[3]
        # Pokretanje programa uz odgovarajuce parametre
        main(input_path, bootstrap_server, topic_name)
