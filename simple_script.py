from pymongo import MongoClient, GEOSPHERE

__author__ = "Innuy"
__version__ = "1.0"

HOST = "localhost"
PORT = "27017"

db = MongoClient("mongodb://" + HOST + ":" + PORT).test


def populate_test_db():
    """
    It connects to the default "test" database and delete all the documents on the People collection, just to know
    the query results result. Then, inserts 3 sample documents on that collection
    """

    print "\nCleaning collection..."

    # Delete all the documents on the People collection
    result = db.People.delete_many({})

    print "Documents deleted: " + str(result.deleted_count)

    bulk = db.People.initialize_unordered_bulk_op()

    bulk.insert({
        "name": {
            "first_name": "Alice",
            "last_name": "Smith"
        },
        "address": {
            "street": "5th Avenue",
            "building": "269",
            "coord": {"type": "Point", "coordinates": [-56.137, -34.901]}
        }
    })

    bulk.insert({
        "name": {
            "first_name": "Janice",
            "last_name": "Smith"
        },
        "address": {
            "street": "4th Avenue",
            "building": "230",
            "coord": {"type": "Point", "coordinates": [-56.135, -34.902]}
        }
    })

    bulk.insert({
        "name": {
            "first_name": "Sarah",
            "last_name": "Parker"
        },
        "address": {
            "street": "1st Avenue",
            "building": "183",
            "coord": {"type": "Point", "coordinates": [-56.242, -35.011]}
        }
    })

    bulk.execute()

    db.People.create_index([
        ("address.coord", GEOSPHERE)
    ])


def print_document(document):
    """
    Prints a Mongo document from the People collection
    :param document: Document to be printed
    """
    name = document.get("name")
    address = document.get("address")
    try:
        print name.get("first_name") + " " + name.get("last_name") + ", " + \
              address.get("street") + " " + address.get("building")
    except Exception:
        print "Some of the user's information could't be retrieved"

    if document.get("distance"):
        print "\tDistance: " + str(round(document.get("distance"), 2)) + " mts"


def simple_queries():
    """
    It gets and prints all the documents in the database. Then, it lists all the names of the people
    whose last name is Smith
    """
    print "\nPeople in the db:"

    cursor = db.People.find()

    for document in cursor:
        print_document(document)

    print "\nPeople whose last name is Smith:"

    cursor = db.People.find({"name.last_name": "Smith"})

    for document in cursor:
        try:
            print document.get("name").get("first_name")
        except Exception:
            print "Smith first name wasn't found"


def simple_updates():
    """
    It modifies every person whose last name is Smith to Johnson instead. Then, it replace one person called John to
    be Clark Kent, and if no one is called that way, it adds Clark Kent to the database.
    """
    print "\nUpdating database..."

    result = db.People.update_many({"name.last_name": "Smith"},
                                   {
                                       "$set": {
                                           "name.last_name": "Johnson"
                                       }
                                   })

    print "\nPeople Smith modified: " + str(result.modified_count)

    result = db.People.replace_one({"name.first_name": "John"},
                                   {
                                       "name": {
                                           "first_name": "Clark",
                                           "last_name": "Kent"
                                       },
                                       "address": {
                                           "street": "Metropolis Avenue",
                                           "building": "204",
                                           "coord": {"type": "Point", "coordinates": [-57.251, -39.015]}
                                       }
                                   }, upsert=True)

    print "People called John: " + str(result.matched_count)


def geospatial_queries():
    """It find people near a predefined coordinate, within a max distance of 1000ft (304.8 mts). Then, it finds person
    near Sarah Parker within 200ft (60.96 mts)"""
    longitude = -56.134
    latitude = -34.9
    first_distance = 304.8  # 1000ft in mts
    second_distance = 60.96  # 200ft in mts

    print "\nPeople near (" + str(longitude) + ", " + str(latitude) + ")"

    cursor = db.People.aggregate([{
        "$geoNear": {
            "near": {"type": "Point", "coordinates": [longitude, latitude]},
            "spherical": True,
            "distanceField": "distance",
            "maxDistance": first_distance
        }
    }])

    for document in cursor:
        print_document(document)

    print "\nPeople near Sarah Parker"

    cursor = db.People.find({
        "name.first_name": "Sarah",
        "name.last_name": "Parker"
    })

    try:
        coordinates = cursor[0].get("address").get("coord").get("coordinates")
        sarah_id = cursor[0].get("_id")

        cursor = db.People.aggregate([{
            "$geoNear": {
                "near": {"type": "Point", "coordinates": coordinates},
                "spherical": True,
                "distanceField": "distance",
                "maxDistance": second_distance,
                "query": {
                    "_id": {"$ne": sarah_id}  # Excludes Sarah Parker from the query
                }
            }
        }])

        i = 0
        for document in cursor:
            print_document(document)
            i += 1

        if i != 0:
            print "Nobody"

    except KeyError:
        print "No Sarah's found"
    except AttributeError:
        print "No coordinate was found"


populate_test_db()
simple_queries()
simple_updates()
simple_queries()
geospatial_queries()
