import {
  sparqlEscapeUri,
  uuid,
  sparqlEscapeString,
  sparqlEscapeDateTime,
  sparqlEscapeInt,
} from "mu";
import { appendFile, stat } from "fs/promises";
import { basename } from "path";

import { querySudo as query, updateSudo as update } from "@lblod/mu-auth-sudo";
import { PUBLISHER_URI, HIGH_LOAD_DATABASE_ENDPOINT } from "../constants";
const connectionOptions = {
  sparqlEndpoint: HIGH_LOAD_DATABASE_ENDPOINT,
  mayRetry: true,
};
export async function getFilePath(remoteFileUri) {
  const result = await query(
    `
    PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>

    SELECT ?file
    WHERE {
      ?file nie:dataSource ${sparqlEscapeUri(remoteFileUri)} .
    } LIMIT 1`,
    {},
    connectionOptions,
  );
  if (result.results.bindings.length) {
    const file = result.results.bindings[0]["file"].value;
    console.log(`Getting contents of file ${file}`);
    const path = file.replace("share://", "/share/");
    return path;
  }
}

export async function getFileMetadata(remoteFileUri) {
  //TODO: needs extension if required
  // prettier-ignore
  const result = await query(`
      SELECT DISTINCT ?url WHERE{
         GRAPH ?g {
           ${sparqlEscapeUri(remoteFileUri)} <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#url> ?url.
         }
      }
  `, {}, connectionOptions);

  if (result.results.bindings.length) {
    const url = result.results.bindings[0]["url"].value;
    return { url };
  } else {
    return null;
  }
}

export async function appendTempFile(content, path) {
  try {
    await appendFile(path, content, "utf-8");
  } catch (e) {
    console.log(`Failed to append TTL to file <${path}>.`);
    throw e;
  }
}

export async function writeToFile(
  graph,
  path,
  logicalFileName,
  derivedFrom,
  extension = "ttl",
  contentType = "text/turtle",
) {
  const phyId = uuid();
  const phyFilename = basename(path);
  const physicalFile = path.replace("/share/", "share://");
  const loId = uuid();
  const logicalFile = `http://data.lblod.info/id/files/${loId}`;
  const now = new Date();

  try {
    const stats = await stat(path);
    const fileSize = stats.size;

    // prettier-ignore
    await update(`
      PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
      PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
      PREFIX dct: <http://purl.org/dc/terms/>
      PREFIX prov: <http://www.w3.org/ns/prov#>
      PREFIX dbpedia: <http://dbpedia.org/ontology/>
      INSERT DATA {
        GRAPH ${sparqlEscapeUri(graph)} {
          ${sparqlEscapeUri(physicalFile)} a nfo:FileDataObject;
                                  nie:dataSource ${sparqlEscapeUri(logicalFile)} ;
                                  mu:uuid ${sparqlEscapeString(phyId)};
                                  nfo:fileName ${sparqlEscapeString(phyFilename)} ;
                                  dct:creator <http://lblod.data.gift/services/harvesting-import-service>;
                                  dct:created ${sparqlEscapeDateTime(now)};
                                  dct:modified ${sparqlEscapeDateTime(now)};
                                  dct:publisher ${sparqlEscapeUri(PUBLISHER_URI)};
                                  dct:format "${contentType}";
                                  nfo:fileSize ${sparqlEscapeInt(fileSize)};
                                  dbpedia:fileExtension "${extension}".
          ${sparqlEscapeUri(logicalFile)} a nfo:FileDataObject;
                                  mu:uuid ${sparqlEscapeString(loId)};
                                  nfo:fileName ${sparqlEscapeString(logicalFileName)} ;
                                  dct:creator <http://lblod.data.gift/services/harvesting-import-service>;
                                  dct:created ${sparqlEscapeDateTime(now)};
                                  prov:wasDerivedFrom ${sparqlEscapeUri(derivedFrom.value)};
                                  dct:publisher ${sparqlEscapeUri(PUBLISHER_URI)};
                                  dct:modified ${sparqlEscapeDateTime(now)};
                                  dct:format "${contentType}";
                                  nfo:fileSize ${sparqlEscapeInt(fileSize)};
                                  dbpedia:fileExtension "${extension}" .
        }
      }
`, {}, connectionOptions);
  } catch (e) {
    console.log(
      `Failed to write TTL resource <${phyFilename}> to triplestore.`,
    );
    throw e;
  }

  return logicalFile;
}
