from pymongo import MongoClient
from pprint import pprint
from datetime import datetime

print("\n" + "="*50)
print("Connexion à la base de données :")
print("="*50)
	

# (a) Connexion à MongoDB avec authentification
client = MongoClient(
    host="127.0.0.1",
    port=27017,
    username="datascientest",
    password="dst123"
)

print("\n" + "_"*50)
print("a) Connexion à MongoDB avec authentification : OK")
print("[host='127.0.0.1', port=27017, username='datascientest', password='dst123']")
print("_"*50)


# (b) Liste des bases de données disponibles
print("\n" + "_"*50)
print("b) Liste des bases de données disponibles :")
print("_"*50)
db_list = client.list_database_names()
pprint(db_list, indent=4)

# Accès à la base 'sample' (si elle existe)
if 'sample' in db_list:
    db = client['sample']
    
    # (c) Liste des collections disponibles
    print("\n" + "_"*50)
    print("c) Collections disponibles dans 'sample':")
    print("_"*50)
    collections = db.list_collection_names()
    pprint(collections, indent=4)
    
    # Accès à la collection 'books' (si elle existe)
    if 'books' in collections:
        books_collection = db['books']
        
        # (d) Affichage d'un document de la collection
        print("\n" + "_"*50)
        print("d) Un document de la collection 'books':")
        print("_"*50)
        sample_book = books_collection.find_one()
        pprint(sample_book, indent=4, width=50, depth=3)
    else:
        print("\nLa collection 'books' n'existe pas dans la base 'sample'")
else:
    print("\nLa base de données 'sample' n'existe pas")




print("\n" + "="*50)
print("Exploration de la base :")
print("="*50)

# (a) Afficher le nombre de livres avec plus de 400 pages, affichez ensuite le nombre de livres ayant plus de 400 pages ET qui sont publiés + Afficher le nombre de livres ayant le mot-clé Android dans leur description 
print("\n" + "_"*50)
print("a) Afficher le nombre de livres avec plus de 400 pages, affichez ensuite le nombre de livres ayant plus de 400 pages ET qui sont publiés + Afficher le nombre de livres ayant le mot-clé Android dans leur description :")
db = client['sample']
books = db['books']

# Question 1: Nombre de livres avec plus de 400 pages
nb_livres_400_pages = books.count_documents({"pageCount": {"$gt": 400}})
print(f"Nombre de livres avec plus de 400 pages: {nb_livres_400_pages}")

# Question 2: Nombre de livres avec plus de 400 pages ET publiés
nb_livres_400_pages_publies = books.count_documents({
    "pageCount": {"$gt": 400},
    "status": "PUBLISH"
})
print(f"Nombre de livres avec plus de 400 pages ET publiés: {nb_livres_400_pages_publies}")
print("_"*50)

# (b)  Afficher le nombre de livres ayant le mot-clé Android dans leur description 
print("\n" + "_"*50)
print("b)  Afficher le nombre de livres ayant le mot-clé Android dans leur description :")
nb_livres_android = books.count_documents({
    "$or": [
        {"shortDescription": {"$regex": "Android", "$options": "i"}},
        {"longDescription": {"$regex": "Android", "$options": "i"}}
    ]
})

print(f"Nombre de livres mentionnant 'Android' dans leur description: {nb_livres_android}")
print("_"*50)


# (c)  Grouper tous les documents - Créer 2 sets à partir des catégories contenus dans la liste categories selon leur index 0 ou 1
print("\n" + "_"*50)
print("c) Grouper tous les documents - Créer 2 sets à partir des catégories contenus dans la liste categories selon leur index 0 ou 1 :")
# Pipeline d'agrégation
pipeline = [
    {
        "$group": {
            "_id": None,
            "categories_index_0": {
                "$addToSet": {
                    "$arrayElemAt": ["$categories", 0]
                }
            },
            "categories_index_1": {
                "$addToSet": {
                    "$arrayElemAt": ["$categories", 1]
                }
            }
        }
    }
]

# Exécution de l'agrégation
result = list(books.aggregate(pipeline))[0]  # Récupère le premier (et seul) résultat

# Affichage des résultats
print("\nCatégories (index 0):")
pprint(result["categories_index_0"])

print("\nCatégories (index 1):")
pprint(result["categories_index_1"])

print("_"*50)

# (d)  Afficher le nombre de livres qui contiennent des noms de langages suivant dans leur description longue : Python, Java, C++, Scala
print("\n" + "_"*50)
print("d)  Afficher le nombre de livres qui contiennent des noms de langages suivant dans leur description longue : Python, Java, C++, Scala :")
# Liste des langages à rechercher
query = {
    "longDescription": {
        "$regex": "Python|Java|C\\+\\+|Scala",
        "$options": "i"
    }
}
count = books.count_documents(query)

# Comptage des résultats
count = books.count_documents(query)

# Affichage du résultat
print(f"Nombre de livres mentionnant Python, Java, C++ ou Scala dans leur description longue : {count}")

print("_"*50)

# (e)  Afficher diverses informations statistiques sur notre bases de données : nombre maximal, minimal, et moyen de pages par catégorie
print("\n" + "_"*50)
print("e)  Afficher diverses informations statistiques sur notre bases de données : nombre maximal, minimal, et moyen de pages par catégorie :")

# Pipeline d'agrégation
pipeline = [
    # Étape 1 : Filtrer les livres avec 'pageCount' et 'categories' valides
    {
        "$match": {
            "pageCount": {"$exists": True, "$ne": None},
            "categories": {"$exists": True, "$ne": []}
        }
    },
    # Étape 2 : Éclater le tableau 'categories' pour traiter chaque catégorie individuellement
    {
        "$unwind": "$categories"
    },
    # Étape 3 : Grouper par catégorie et calculer les stats
    {
        "$group": {
            "_id": "$categories",  # Groupe par catégorie
            "max_pages": {"$max": "$pageCount"},  # Nombre max de pages
            "min_pages": {"$min": "$pageCount"},  # Nombre min de pages
            "avg_pages": {"$avg": "$pageCount"}    # Moyenne de pages
        }
    },
    # Étape 4 (Optionnelle) : Trier par catégorie
    {
        "$sort": {"_id": 1}
    }
]

# Exécution de l'agrégation
results = list(books.aggregate(pipeline))

# Affichage des résultats
print("Statistiques du nombre de pages par catégorie:\n")
for stat in results:
    print(
        f"Catégorie: {stat['_id']}\n"
        f"  - Max: {stat['max_pages']} pages\n"
        f"  - Min: {stat['min_pages']} pages\n"
        f"  - Moyenne: {stat['avg_pages']:.1f} pages\n"
    )

print("_"*50)


# (f) Filtrer seulement les livres publiés après 2009
print("\n" + "_"*50)
print("f)  Filtrer seulement les livres publiés après 2009 :")
# Pipeline d'agrégation
pipeline = [
    # Étape 1: Normaliser les dates (string et date)
    {
        "$addFields": {
            "normalizedDate": {
                "$cond": [
                    {"$eq": [{"$type": "$publishedDate"}, "string"]},
                    {"$dateFromString": {
                        "dateString": "$publishedDate",
                        "format": "%Y-%m-%d"  
                    }},
                    "$publishedDate"  # si la date est une vrai date
                ]
            }
        }
    },
    # Étape 2: Filtrer les dates après 2009
    {
        "$match": {
            "normalizedDate": {
                "$gte": datetime(2010, 1, 1),
                "$lte": datetime.now()
            }
        }
    },
    # Étape 3: Extraire les composantes de date
    {
        "$project": {
            "title": 1,
            "original_date": "$normalizedDate",
            "year": {"$year": "$normalizedDate"},
            "month": {"$month": "$normalizedDate"},
            "day": {"$dayOfMonth": "$normalizedDate"},
            "_id": 0
        }
    },
    # Étape 4: Trier sur la date
    {
        "$sort": {"original_date": -1}
    },
    # Étape 5: Limiter aux 20 premiers résultats
    {
        "$limit": 20
    }
]

results = list(books.aggregate(pipeline))

print("\nLivres publiés après 2009 (20 plus récents):")
pprint(results, width=120, compact=True)
print("_"*50)


# (g) À partir de la liste des auteurs, créez de nouveaux attributs (author_1, author_2 ... author_n)
print("\n" + "_"*50)
print("g) À partir de la liste des auteurs, créez de nouveaux attributs (author_1, author_2 ... author_n) :")

# Pipeline d'agrégation
pipeline = [
    # Étape 1: Filtrer les documents avec des auteurs et une date valide
    {
        "$match": {
            "authors": {"$exists": True, "$ne": []},
            "publishedDate": {"$exists": True}
        }
    },
    # Étape 2: Convertir publishedDate si nécessaire
    {
        "$addFields": {
            "normalizedDate": {
                "$cond": [
                    {"$eq": [{"$type": "$publishedDate"}, "string"]},
                    {"$dateFromString": {"dateString": "$publishedDate"}},
                    "$publishedDate"
                ]
            }
        }
    },
    # Étape 3: Créer les attributs author_1, author_2, etc.
    {
        "$addFields": {
            "author_1": {"$arrayElemAt": ["$authors", 0]},
            "author_2": {"$arrayElemAt": ["$authors", 1]},
            "author_3": {"$arrayElemAt": ["$authors", 2]},
            "author_4": {"$arrayElemAt": ["$authors", 3]},
            "total_authors": {"$size": "$authors"}
        }
    },
    # Étape 4: Trier par date de publication (chronologique)
    {
        "$sort": {"normalizedDate": 1}
    },
    # Étape 5: Projection pour ne garder que les champs utiles
    {
        "$project": {
            "title": 1,
            "authors": 1,
            "publishedDate": "$normalizedDate",
            "author_1": 1,
            "author_2": 1,
            "author_3": 1,
            "author_4": 1,
            "total_authors": 1,
            "_id": 0
        }
    },
    # Étape 6: Limiter aux 20 premiers résultats
    {
        "$limit": 20
    }
]

results = list(books.aggregate(pipeline))
print("\ncréez de nouveaux attributs (author_1, author_2 ... author_n) :\n")
pprint(results, width=120, compact=True)

print("*"*10)
# COMPORTEMENT DU PIPELINE 
print("COMPORTEMENT DU PIPELINE:")
print("1.'authors': ['William R. Cockayne and Michael Zyda', 'editors'] : plusieurs auteurs dans le titre")
print("2.'authors': ['Margaret M. Burnett', 'Adele Goldberg', '', 'Ted G. Lewis'] : auteur vide dans le titre")
print("*"*10)
print("_"*50)

# (g) créer une colonne contenant le nom du premier auteur, puis agréger selon cette colonne pour obtenir le nombre d'articles pour chaque premier auteur. Afficher le nombre de publications pour les 10 premiers auteurs les plus prolifiques
print("\n" + "_"*50)
print("g) créer une colonne contenant le nom du premier auteur, puis agréger selon cette colonne pour obtenir le nombre d'articles pour chaque premier auteur. Afficher le nombre de publications pour les 10 premiers auteurs les plus prolifiques :")

print("=== Premier auteur de chaque document ===")
# Affichage du premier auteur pour chaque document (limité aux 5 premiers pour l'exemple)
for book in books.find({"authors": {"$exists": True, "$ne": []}}).limit(5):
    first_author = book['authors'][0] if book['authors'] else "Aucun"
    print(f"Titre: {book.get('title', 'Inconnu')} | Premier auteur: {first_author}")

print("\n=== Calcul du nombre de publications par premier auteur ===")
print("Démarrage du calcul...")

# Pipeline d'agrégation
pipeline = [
    # Étape 1: Filtrer les documents avec des auteurs
    {
        "$match": {
            "authors": {"$exists": True, "$ne": []}
        }
    },
    # Étape 2: Extraire le premier auteur
    {
        "$project": {
            "first_author": {"$arrayElemAt": ["$authors", 0]},
            "title": 1
        }
    },
    # Étape 3: Compter les publications par auteur
    {
        "$group": {
            "_id": "$first_author",
            "publication_count": {"$sum": 1},
            "titles": {"$push": "$title"}  # Optionnel: liste des titres
        }
    },
    # Étape 4: Trier par nombre de publications (décroissant)
    {
        "$sort": {"publication_count": -1}
    },
    # Étape 5: Limiter aux 10 premiers
    {
        "$limit": 10
    },
    # Étape 6: Formater les résultats
    {
        "$project": {
            "Auteur": "$_id",
            "Nombre de publications": "$publication_count",
            "_id": 0
        }
    }
]



# Exécution de l'agrégation
results = list(books.aggregate(pipeline))
    
print("\n=== TOP 10 des auteurs les plus prolifiques ===")
print("Classement | Auteur | Nombre de publications")
print("---------------------------------------------")
for rank, author in enumerate(results, 1):
	print(f"{rank:>9} | {author['Auteur']:<20} | {author['Nombre de publications']}")
    

# Fermeture de la connexion
client.close()

