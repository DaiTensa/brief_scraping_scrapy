# brief_scraping_scrapy

## Contexte
Vous êtes cinéphile et vous souhaitez vous créer une base de données personnelle pour rechercher vos films et séries préférés.

## Objectifs
- Récolter des données en scrapant le site web [Allociné](https://www.allocine.fr)..
- Constituer une base de donnée.
- Intégrer la base de donnée dans [Azure](https://azure.microsoft.com).
- Automatisation du scrapping, et prévoir une pipeline de nettoyage des données.
- Stocker les données dans une base de données Postgres sous [Azure](https://azure.microsoft.com).

## Les données à scrapper

*Films*
- Titre: Titre du film
- Titre original : Titre original du film
- Score Presse : Note de la presse spécialisée 
- Score Utilisateurs : Note des utilisateurs
- Genre : Genre du film (comédie, drame, etc.) 
- Type : Type du film (Long métrage, etc.)
- Année : Année de sortie du film 
- Durée : Durée du film (en minutes) 
- Description : Synopsis du film 
- Acteurs : Liste des acteurs principaux 
- Réalisateur : Nom du réalisateur 
- Public : Classification du film (Tous publics, etc.) 
- Pays d'origine : Pays d'origine du film

*Pour les séries*
en plus des informations des Films
- Nombre de saisons : Nombre total de saisons
- Nombre d'épisodes : Nombre total d'épisodes

## Définition du modèle MCD (Modèle Conceptuel des Données)
### Les concepts
La prmière étape consiste à définr notre MCD, nous avons 

```mermaid
---
title: MCD Scrapping Allociné
---
erDiagram
    Film {}
    
    Serie {}
    
    PERSONNES {}
    
    FILMCASTING {}

    SERIECASTING {}
```

### Les attributs
La prmière étape consiste à définr notre MCD, nous avons 

```mermaid
---
title: MCD Scrapping Allociné
---
erDiagram
    Film {
        int IdFilm PK
        string Title
        string TitleOrigine
        string Synopsis
        datetime DateSortie
        int Duree
        string Directors
        string Casting
        string Langues
        int Annee
        int Type
        int Genre
        datetime TimeItem
        string NoteUser
        float NoteUser
        int UserNoteCount
        float NotePress
        int PressNoteCount
        string CumulBoxOffice
      
    }
    
    Serie {
        int IdSerie PK
        string Title
        string TitleOrigine
        string Synopsis
        string Date
        int Duree
        string Genre
        string Saison
        string Directors
        string Nationality
        int SaisonCount
        int EpisodeCount
        string Casting
    }
    
    PERSONNES {
        int IdPersonne PK
        string Nom_Prenom
        string Role
    }
    
    FILMCASTING {
        int IdFilm FK
        int IdPersonne FK
    }
    
    SERIECASTING {
        int IdSerie FK
        int IdPersonne FK
        int Saison
    }
```


## Les associations
 **Films et Series**

```mermaid
---
title: MCD Scrapping Allociné
---
erDiagram
    Film {
        int IdFilm PK
        string Title
        string TitleOrigine
        string Synopsis
        datetime DateSortie
        int Duree
        string Directors
        string Casting
        string Langues
        int Annee
        int Type
        int Genre
        datetime TimeItem
        string NoteUser
        float NoteUser
        int UserNoteCount
        float NotePress
        int PressNoteCount
        string CumulBoxOffice
      
    }
    
    Serie {
        int IdSerie PK
        string Title
        string TitleOrigine
        string Synopsis
        string Date
        int Duree
        string Genre
        string Saison
        string Directors
        string Nationality
        int SaisonCount
        int EpisodeCount
        string Casting
    }
    
    PERSONNES {
        int IdPersonne PK
        string Nom_Prenom
        string Role
    }
    
    FILMCASTING {
        int IdFilm FK
        int IdPersonne FK
    }
    
    SERIECASTING {
        int IdSerie FK
        int IdPersonne FK
        int Saison
    }
Film ||--o{ FILMCASTING : dans
PERSONNES ||--o{ FILMCASTING : joue
PERSONNES ||--o{ SERIECASTING : joue
Serie ||--o{ SERIECASTING : dans
```
