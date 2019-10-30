# Fixed File Format converter

L'objectif de cet exercice est d'écrire un outil générique qui convertira un fichier d'entrée au format fixe en un fichier csv, en se basant sur un fichier de metadonnées décrivant sa structure.

Vous êtes libre d'utiliser n'importe quel langage ou librairie open source si vous en avez besoin.
Créez un fork de ce projet et fournissez nous votre code complet en pull request (en incluant le code source et les tests)

## Cas d'usage

Notre fichier d'entrée peut avoir n'importe quel nombre de colonnes
Une colonne peut-être d'un de ces 3 formats:
* date (format yyyy-mm-dd)
* numerique (séparateur décimal '.', peut être négatif)
* string

La structure du fichier est définie dans un fichier de métadonnées, au format csv, où chaque ligne décrit chaque colonne:
* nom de la colonne
* taille de la colonne
* type de la colonne

Vous devez transformer le fichier d'entrée en un fichier csv (séparateur ',' et séparateur de ligne CRLF)

Les dates doivent être formatés en dd/mm/yyyy

Les espaces en fin de chaine de caractères doivent être nettoyés (trim)

La première ligne du fichier csv doit être le nom des colonnes

## Exemple

Fichier d'entrée :
```
1970-01-01John           Smith           81.5
1975-01-31Jane           Doe             61.1
1988-11-28Bob            Big            102.4
```

Fichier de métadonnées :
```
Birth date,10,date
First name,15,string
Last name,15,string
Weight,5,numeric
```

Fichier csv de sortie :
```
Birth date,First name,Last name,Weight
01/01/1970,John,Smith,81.5
31/01/1975,Jane,Doe,61.1
28/11/1988,Bob,Big,102.4
```

## Conditions supplémentaires
* les fichiers sont encodés en UTF-8 et peuvent contenir des caractères spéciaux
* les colonnes au format string peuvent contenir des séparateurs ','. Dans ce cas la chaîne de caractères complète doit être protégée par des " (double quote) 
* dans le cas où le format de fichier n'est pas correct, le programme doit échouer en expliquant la raison
* le fichier d'entrée peut être très volumineux (plusieurs Go)

## Que voulons nous évaluer à travers ce test ? ##

La manière de résoudre le problème et l'utilisation des bonnes pratiques de craft et de clean code.

## Qu'est-ce que nous n'évaluons pas ? ##

Les frameworks et technologies utilisés
