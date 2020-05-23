import sys
import re
import json
from glob import glob
from datetime import datetime


# Main funkcija koja prima kao argumente putanje do foldera
# gde se redom nalaze fajlovi vezani za zagadjenje, saobracaj i vreme,
# kao i putanju do foldera gde ce biti napravljeni izlazni fajlovi
def main(pollution_input_path, traffic_input_path, weather_input_path, output_path):

    # Liste fajlova za zagadjenje i saobracaj, koji treba da ucestvuju u spajanju
    pollution_files = glob(pollution_input_path + "/*.csv")
    traffic_files = glob(traffic_input_path + "/*csv")

    # Za svaki fajl se izvuce identifikator, kako bi moglo da se izvrsi njihovo uparivanje
    pollution_ids = list(map(lambda x: re.search(
        r'\d+\.csv', x).group(), pollution_files))
    traffic_ids = list(map(lambda x: re.search(
        r'\d+\.csv', x).group(), traffic_files))

    # Uparuju se fajlovi, tj. za svaki fajl o zagadjenju
    # nalazi se odgovarajuci fajl o saobracaju sa istim identifikatorom
    paired_files = [(x, next((traffic_files[y] for y in range(len(traffic_ids)) if traffic_ids[y] ==
                              pollution_ids[index]), None), pollution_ids[index]) for index, x in enumerate(pollution_files)]
    # Izbacuju se eventualni neupareni fajlovi
    paired_files = list(filter(lambda x: x[1] is not None, paired_files))

    # Inicijalizuje se recnik koji za svaku vremensku karakteristiku pamti
    # spisak izmerenih vrednosti u odredjenim vremenskim trenucima
    weather_points = dict()
    # Dati niz pamti imena vremenskih karakteristika
    weather_measurements = ["dewptm", "hum",
                            "pressurem", "tempm", "wdird", "wspdm"]
    # Za svaku vremensku karakteristiku obilazi se fajl koji pamti vrednosti
    for measurement in weather_measurements:
        # Inicijalizuje se niz koji ce da pamti odgovarajuce izmerene vrednosti
        weather_points[measurement] = []
        with open(weather_input_path + "/dewptm.txt", 'r') as opened_weather_file:
            for line in opened_weather_file:
                # Ucitavaju se izmerene vrednosti koje odgovaraju liniji iz fajla,
                # tj. nekom konkretnom danu
                day = json.loads(line)
                for key in sorted(day):
                    # Ako je vrednost validna, tj. ako nije dobijen prazan string...
                    if day[key] != "":
                        # ... pamti se ta izmerena vrednost, zajedno uz vremenski trenutak
                        weather_points[measurement].append(
                            (datetime.strptime(key, "%Y-%m-%dT%H:%M:%S"), float(day[key])))

    # Za svaki par fajlova o zagadjenju i saobracaju,
    # neophodno je generisati izlazni spojen fajl
    for pollution_file, traffic_file, suffix in paired_files:
        # Za svaku vremensku karakteristiku, pamti se pocetni i krajnji indeks,
        # koji odredjuju dve izmerene vrednosti pomocu kojih ce ona biti aproksimirana
        # (granularnost koja se trazi u fajlovima za zagadjenje i saobracaj
        # nije ista kao ona koja je data u fajlovima za vremenske karakteristike, tj.
        # one su dosta redje merene nego sto treba, tako da je neophodno
        # vrsiti aproksimaciju vrednosti uz pomoc poznatih vrednosti)
        starting = dict()
        ending = dict()
        # Indeksi svih vremenskih karakteristika mogu da se inicijalizuju na pocetak,
        # zato sto su vremenski trenuci unutar svakog fajla sortirani po rastucem redosledu
        for measurement in weather_measurements:
            starting[measurement] = 0
            ending[measurement] = 0
        # Istovremeno se otvaraju odgovarajuci fajl koji pamti podatke o zagadjenju,
        # fajl koji pamti podatke o saobracaju, i kreira se novi fajl
        # koji treba da pamti rezultat spajanja
        with open(pollution_file, 'r') as opened_pollution, open(traffic_file, 'r') as opened_traffic, open(output_path + "/merged" + suffix, 'w+') as opened_merged:
            # Upisuje se header u fajl
            opened_merged.write(
                'Timestamp, Latitude, Longitude, Average_speed, Vehicle_count, Is_Monday, Is_Tuesday, Is_Wednesday, Is_Thursday, Is_Friday, Is_Saturday, Is_Sunday, Dew_point, Humidity, Pressure, Temperature, Wind_direction, Wind_speed, Ozone, Particulate_matter, Carbon_monoxide, Sulfur_dioxide, Nitrogen_dioxide\n')
            # Ucitavaju se pocetne linije iz oba fajla
            pollution_line = opened_pollution.readline()
            traffic_line = opened_traffic.readline()
            while True:
                # Ucitavanje se nastavlja sve dok se ne dodje do kraja bar jednog od fajlova
                if pollution_line == "" or traffic_line == "":
                    break
                # Linije se dele na reci (zarez predstavlja delimiter)
                pollution_split = pollution_line.strip().split(",")
                traffic_split = traffic_line.strip().split(",")

                # Ukoliko je ucitan header u nekom od fajlova, on se preskace
                if "timestamp" in pollution_split[-1].lower():
                    pollution_line = opened_pollution.readline()
                    continue
                if "timestamp" in traffic_split[5].lower():
                    traffic_line = opened_traffic.readline()
                    continue

                # Racuna se timestamp, tj. vremenski trenutak
                # koji odgovara ucitanim rekordima iz oba fajla
                pollution_time = datetime.strptime(
                    pollution_split[-1], "%Y-%m-%d %H:%M:%S")
                traffic_time = datetime.strptime(
                    traffic_split[5], "%Y-%m-%dT%H:%M:%S")

                # Rekord iz fajla o zagadjenju je stariji i neuparen,
                # tako da treba da se ignorise
                if pollution_time < traffic_time:
                    pollution_line = opened_pollution.readline()
                    continue
                # Rekord iz fajla o saobracaju je stariji i neuparen,
                # tako da treba da se ignorise
                elif pollution_time > traffic_time:
                    traffic_line = opened_traffic.readline()
                    continue
                # Rekordi imaju isti timestamp i treba da se upare
                else:
                    # Inicijalizuje se recnik koji pamti vrednosti vremenskih karakteristika
                    weather_values = dict()
                    for measurement in weather_measurements:
                        # Pocetni indeks pokazuje na najkasniji izmereni timestamp vremenske karakteristike
                        # koji nije posle trazenog vremenskog trenutka
                        while starting[measurement] + 1 < len(weather_points[measurement]) and weather_points[measurement][starting[measurement]+1][0] <= pollution_time:
                            starting[measurement] += 1
                        # Krajnji indeks pokazuje na najraniji izmereni timestamp vremenske karakteristike
                        # koji nije pre trazenog vremenskog trenutka
                        while ending[measurement] < len(weather_points[measurement]) - 1 and weather_points[measurement][ending[measurement]][0] < pollution_time:
                            ending[measurement] += 1
                        # Ako se poklapaju indeksi, znaci da za trazeni timestamp
                        # upravo postoji tacna izmerena vrednost vremenske karakteristike
                        if starting[measurement] == ending[measurement]:
                            weather_values[measurement] = weather_points[measurement][starting[measurement]][1]
                        # U suprotnom, tacna vrednost ne postoji,
                        # i neophodno je izvrsiti linearnu aproksimaciju
                        # pomocu najblizih poznatih vrednosti
                        else:
                            coeff1 = (
                                weather_points[measurement][ending[measurement]][0] - pollution_time).total_seconds()
                            coeff2 = (
                                pollution_time - weather_points[measurement][starting[measurement]][0]).total_seconds()
                            weather_values[measurement] = (
                                coeff1 * weather_points[measurement][starting[measurement]][1] + coeff2 * weather_points[measurement][ending[measurement]][1])/(coeff1+coeff2)

                    # Kreira se rekord koji ce biti upisan u novokreirani fajl
                    record = [pollution_split[-1].strip(),
                              pollution_split[-2].strip(),
                              pollution_split[-3].strip(),
                              traffic_split[2].strip(),
                              traffic_split[6].strip(),
                              str(int(pollution_time.weekday() == 0)),
                              str(int(pollution_time.weekday() == 1)),
                              str(int(pollution_time.weekday() == 2)),
                              str(int(pollution_time.weekday() == 3)),
                              str(int(pollution_time.weekday() == 4)),
                              str(int(pollution_time.weekday() == 5)),
                              str(int(pollution_time.weekday() == 6)),
                              str(weather_values["dewptm"]),
                              str(weather_values["hum"]),
                              str(weather_values["pressurem"]),
                              str(weather_values["tempm"]),
                              str(weather_values["wdird"]),
                              str(weather_values["wspdm"]),
                              pollution_split[0].strip(),
                              pollution_split[1].strip(),
                              pollution_split[2].strip(),
                              pollution_split[3].strip(),
                              pollution_split[4].strip()
                              ]

                    # Upisuje se rekord u fajl
                    opened_merged.write(", ".join(record) + "\n")
                    # Ucitavaju se naredne linije u oba pocetna fajla
                    pollution_line = opened_pollution.readline()
                    traffic_line = opened_traffic.readline()

        # Na konzoli se prikazuje putanja do novokreiranog fajla
        # nakon sto se njegova obrada (tj. popunjavanje) zavrsi
        print(output_path + "/merged" + suffix)


# Sintaksa za pokretanje programa s komandne linije je:
# python merger.py pollution_input_path traffic_input_path weather_input_path output_path
# Na primer:
# python merger.py /home/aleksandra.stojnev/apps/ivan.damnjanovic/pollution /home/aleksandra.stojnev/apps/ivan.damnjanovic/traffic ...
# ... /home/aleksandra.stojnev/apps/ivan.damnjanovic/weather /home/aleksandra.stojnev/apps/ivan.damnjanovic/merged
if __name__ == "__main__":
    if len(sys.argv) != 5:
        # Greska nastupa ukoliko sintaksa nije dobra, tj. nema tacno 4 parametra
        print("\nBad command syntax. The correct parameter syntax is:")
        print("pollution_input_path traffic_input_path weather_input_path output_path\n")
    else:
        pollution_input_path = sys.argv[1]
        traffic_input_path = sys.argv[2]
        weather_input_path = sys.argv[3]
        output_path = sys.argv[4]
        # Pokretanje programa uz odgovarajuce parametre
        main(pollution_input_path, traffic_input_path,
             weather_input_path, output_path)
