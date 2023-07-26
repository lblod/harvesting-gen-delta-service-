import { sparqlEscapeUri, sparqlEscapeString, uuid } from 'mu';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import {
  PREFIXES,
  STATUS_BUSY,
  STATUS_SUCCESS,
  STATUS_FAILED,
} from '../constants';

import { appendJsonFile, getTriplesInFileAndApplyByBatch } from './file-helpers';
import { loadExtractionTask, updateTaskStatus, appendTaskError } from './task';
import { join } from 'path';

import { HIGH_LOAD_DATABASE_ENDPOINT, TARGET_DELTA_GRAPH, TARGET_DIRECTORY_DELTA_PATH } from '../constants';

const n3ToDelta = (triple) => {
  const newTriple = {
    subject: {
      value: triple.subject.value,
      type: triple.subject.type,
    },
    predicate: {
      value: triple.predicate.value,
      type: triple.predicate.type,
    },
    object: {
      value: triple.object.value,
      type: triple.object.type,
    }

  };
  if (triple.subject.datatype) {
    newTriple.subject.datatype = triple.subject.datatype;
  }
  if (triple.predicate.datatype) {
    newTriple.predicate.datatype = triple.predicate.datatype;
  }
  if (triple.object.datatype) {
    newTriple.object.datatype = triple.object.datatype;
  }
  return newTriple;
}

const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

export async function run(deltaEntry) {
  const task = await loadExtractionTask(deltaEntry);
  console.debug("Task: \n", task);
  if (!task) return;

  try {
    updateTaskStatus(task, STATUS_BUSY);

    const skipDeletes = []; // in case of array, we produce deletes only once
    const connectionOptions = { sparqlEndpoint: HIGH_LOAD_DATABASE_ENDPOINT, mayRetry: true };

    await getTriplesInFileAndApplyByBatch(task, async (tripleStore) => {
      const deletePage = [];
      const insertPage = [];
      for (let triple of [...tripleStore]) {
        insertPage.push(n3ToDelta(triple));
        if (skipDeletes.some(toSkip => toSkip.subject === triple.subject.value && toSkip.predicate === triple.predicate.value)) {
          continue;
        }
        let result = await query(`${PREFIXES}
         select ?subject ?predicate ?object where {
            graph  ${sparqlEscapeUri(TARGET_DELTA_GRAPH)} {
                BIND(${sparqlEscapeUri(triple.subject.value)}  as ?subject).
                BIND(${sparqlEscapeUri(triple.predicate.value)}  as ?predicate).
                  ?subject ?predicate ?object.
            }
         }`, {}, connectionOptions);

        if (result.results.bindings.length) { // delete statement
          for (let b of result.results.bindings) {
            deletePage.push(b);
          }
          if (result.results.bindings.length > 1) {
            skipDeletes.push({ subject: triple.subject.value, predicate: triple.predicate.value });
          }
        }
      }
      const jsonDelete = {
        deletes: deletePage,
        inserts: []
      };
      const jsonInsert = {
        deletes: [],
        inserts: insertPage
      };
      const json = [jsonDelete, jsonInsert];

      const dateNow = new Date().toISOString();
      const fileName = join(TARGET_DIRECTORY_DELTA_PATH, `delta-${dateNow}.json`);

      const payload = JSON.stringify(json);
      await appendJsonFile(payload, fileName);
      await delay(10); // just to make sure name will be different

    });

    const graphContainer = { id: uuid() };
    graphContainer.uri = `http://redpencil.data.gift/id/dataContainers/${graphContainer.id}`;
    await appendTaskResultGraph(task, graphContainer, { uri: task.inputContainer });

    updateTaskStatus(task, STATUS_SUCCESS);
  } catch (e) {
    console.error(e);
    if (task) {
      await appendTaskError(task, e.message);
      await updateTaskStatus(task, STATUS_FAILED);
    }
  }
}



async function appendTaskResultGraph(task, container, graphUri) {
  const queryStr = `
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
    PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    INSERT DATA {
      GRAPH ${sparqlEscapeUri(task.graph)} {
        ${sparqlEscapeUri(container.uri)} a nfo:DataContainer.
        ${sparqlEscapeUri(container.uri)} mu:uuid ${sparqlEscapeString(container.id)}.
        ${sparqlEscapeUri(container.uri)} task:hasGraph ${sparqlEscapeUri(graphUri)}.

        ${sparqlEscapeUri(task.task)} task:resultsContainer ${sparqlEscapeUri(container.uri)}.
      }
    }
  `;

  await update(queryStr);

}


