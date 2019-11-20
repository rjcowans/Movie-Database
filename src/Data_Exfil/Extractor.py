import json
import csv
import sys

t = []
z = []
actors = []
directors = []


def unpacker(data, ch,intake):
    works = json.loads(data)
    bell = []
    if len(works) != 0:
        for temp in works:
            test = dict(temp)
            test2 = 0
            test3 = ""
            for make in test:
                if type(test.get(make)) is int:
                    test2 = test.get(make)
                else:
                    test3 = test.get(make)
            if ch == 1 and [test2, test3,intake] not in t:
                t.append([test2, test3,intake])
            elif ch == 2 and [test2, test3,intake] not in z:
                z.append([test2, test3,intake])
    return


def unpacker2(data, moid):
    works = json.loads(data)
    if len(works) != 0:
        for temp in works:
            test = dict(temp)
            if test["job"] == "Director":
                directors.append([test["name"],test["id"], test["gender"], moid])
    return


def unpacker3(data, moid):
    works = json.loads(data)
    if len(works) != 0:
        for temp in works:
            test = dict(temp)
            if test["character"] != "":
                actors.append([test["id"], test["name"], test["character"], test["gender"], moid])
    return


def main():
    print("Hello I am parsing the gaint csv called test.csv and output the result to 3 files")
    print("I only decided to pull 6 categories for the engine: ")
    print("Genres, Keywords, Original_Title, Popularity, Vote Average(scale 10), Vote Count")
    print("Also Writing a Actors csv and Directors csv")
    print("I parsed Keywords and Genres to their own seperate Files \n")
    boom = csv.reader(open(sys.argv[1], 'r', encoding="utf8"))
    boom2 = csv.reader(open(sys.argv[2], 'r', encoding='utf8'))
    l = []
    tags = []
    num = 0
    for x in boom:
        if x:
            if num != 0:
                temp = [x[3],x[6], x[8], x[18], x[19]]
                unpacker(x[1], 1, x[3])
                unpacker(x[4], 2, x[3])
                l.append(temp)
            else:
                tags = [x[6], x[8], x[18], x[19]]
        num = num + 1
    num = 0
    for woop in boom2:
        if woop:
            if num != 0:
                unpacker2(woop[3], woop[0])
                unpacker3(woop[2], woop[0])
            else:
                tags2 = [woop[0], woop[1], woop[2], woop[3]]
        num = num + 1

    with open('movies.csv', 'w', encoding='utf8', newline='') as tank:
        gunner = csv.writer(tank)
        # gunner.writerow(tags)
        for x in l:
            gunner.writerow(x)
    with open('genres.csv', 'w', encoding='utf8', newline='') as gank:
        gunner = csv.writer(gank)
        # gunner.writerow(['ID', 'GENRES'])
        for x in t:
            gunner.writerow(x)
    with open('keywords.csv', 'w', encoding='utf8', newline='') as wank:
        gunner = csv.writer(wank)
        # gunner.writerow(['ID', 'Keywords'])
        for x in z:
            gunner.writerow(x)
    with open('directors.csv', 'w', encoding='utf8', newline='') as spank:
        spanker = csv.writer(spank)
        for beam in directors:
            spanker.writerow(beam)
    with open('actor.csv', 'w', encoding='utf8', newline='') as toy:
        toys = csv.writer(toy)
        for act in actors:
            toys.writerow(act)
    print(
        "Parsing is Done please Check filename\nparsed.csv\nkeywords.csv\ngenres.csv\nactor.csv\ndirectors.csv\nThank you")


main()
