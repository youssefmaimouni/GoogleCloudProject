/**
 * Transforms a single line from a CSV file into a JSON string
 * representing a row to be inserted into BigQuery.
 *
 * @param {string} line A single line from a CSV file.
 * @return {string|null} A JSON string representation of the BigQuery row,
 *                         or null to skip the line (e.g., header).
 */
function transform(line) {
    // Sépare la ligne CSV en valeurs.
    // Attention : cette simple division par ',' ne gère pas correctement les virgules
    // à l'intérieur des champs cités (ex: "New York, NY").
    // Si vos données CSV sont simples et n'ont pas ce cas, cela peut suffire.
    // Sinon, une analyse CSV plus robuste pourrait être nécessaire (plus complexe en JS UDF simple).
    var values = line.split(',');

    // 1. Vérification de l'en-tête (adapter aux noms réels de vos en-têtes)
    //    Il est crucial que cette vérification soit robuste.
    //    Si vos fichiers n'ont PAS d'en-tête, supprimez cette section.
    if (values[0].trim().toLowerCase() === 'order_id' &&
        values[1].trim().toLowerCase() === 'client_id') { // Ajouter d'autres vérifications si besoin pour plus de sûreté
        // Logique pour indiquer que c'est l'en-tête et qu'il faut l'ignorer.
        // Dans Dataflow, retourner null signifie généralement que la ligne est ignorée.
        // Vous pouvez aussi logger côté Dataflow (si le template le permet via des logs custom).
        // console.log("Header row skipped: " + line); // Le console.log ici n'ira pas dans les logs GCP directement.
        return null;
    }

    // 2. Vérifier que nous avons le bon nombre de colonnes (robustesse)
    //    Si une ligne n'a pas le bon nombre de colonnes, elle pourrait causer des erreurs.
    var expectedColumnCount = 8; // order_id, client_id, product_id, country, order_date, quantity, unit_price, status
    if (values.length !== expectedColumnCount) {
        // Gérer les lignes malformées. Vous pourriez :
        // - Les ignorer : return null;
        // - Essayer de les corriger si possible (complexe)
        // - Les envoyer à une table d'erreurs BigQuery (nécessiterait une logique de sortie différente)
        // Pour l'instant, on les ignore et on pourrait logger l'erreur si Dataflow le permet.
        // console.error("Malformed line (incorrect column count: " + values.length + "): " + line);
        return null; // Ignorer la ligne malformée pour l'instant
    }

    // 3. Création de l'objet JSON
    var obj = new Object();
    obj.order_id    = values[0].trim();
    obj.client_id   = parseInt(values[1].trim(), 10); // Convertir en entier
    obj.product_id  = parseInt(values[2].trim(), 10); // Convertir en entier
    obj.country     = values[3].trim();
    obj.order_date  = values[4].trim();             // Doit être au format YYYY-MM-DD pour le type DATE de BQ
    obj.quantity    = parseInt(values[5].trim(), 10); // Convertir en entier
    obj.unit_price  = parseFloat(values[6].trim());   // Convertir en nombre à virgule flottante
    obj.status      = values[7].trim();

    // 4. Gestion des erreurs de conversion (optionnel mais recommandé)
    //    Par exemple, si parseInt ou parseFloat échoue, ils retournent NaN.
    if (isNaN(obj.client_id) || isNaN(obj.product_id) || isNaN(obj.quantity) || isNaN(obj.unit_price)) {
        // console.error("Malformed line (numeric conversion error): " + line);
        return null; // Ignorer la ligne avec des erreurs de conversion numérique
    }

    // Le format de date 'YYYY-MM-DD' est directement compatible avec le type DATE de BigQuery.
    // Si vos dates étaient dans un format différent (ex: DD/MM/YYYY), vous auriez besoin de les parser
    // et de les reformater ici. Par exemple (nécessiterait une logique de parsing de date plus complexe) :
    // var dateParts = values[4].trim().split('/'); // si format DD/MM/YYYY
    // if (dateParts.length === 3) {
    //   obj.order_date = dateParts[2] + '-' + dateParts[1] + '-' + dateParts[0]; // YYYY-MM-DD
    // } else {
    //   // console.error("Malformed line (date format error): " + line);
    //   return null; // Ignorer si le format de date est mauvais
    // }


    // 5. Convertir l'objet en chaîne JSON
    var jsonString = JSON.stringify(obj);
    return jsonString;
}