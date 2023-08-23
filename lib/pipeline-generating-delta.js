import { sparqlEscapeUri, sparqlEscapeString, uuid } from 'mu';
import { updateSudo as update } from '@lblod/mu-auth-sudo';
import {
  STATUS_BUSY,
  STATUS_SUCCESS,
  STATUS_FAILED,
} from '../constants';

import { appendJsonFile, getTriplesInFileAndApplyByBatch } from './graph';
import { loadExtractionTask, updateTaskStatus, appendTaskError } from './task';
import { join } from 'path';
import { writeTtlFile } from './file-helpers';
import { TARGET_DIRECTORY_DELTA_PATH } from '../constants';

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

    const graphContainer = { id: uuid() };
    graphContainer.uri = `http://redpencil.data.gift/id/dataContainers/${graphContainer.id}`;
    const makeFileFromTriplestore = fromDeltaToJson => async tripleStore => {
      const page = tripleStore.map(triple => n3ToDelta(triple));
      const json = [fromDeltaToJson(page)];
      const dateNow = new Date().toISOString();
      const logicalName = `delta-${dateNow}.json`;
      const fileName = join(TARGET_DIRECTORY_DELTA_PATH, logicalName);

      const payload = JSON.stringify(json);
      await appendJsonFile(payload, fileName);
      const fileUri = await writeTtlFile(task.graph, fileName, logicalName);

      const fileContainer = { id: uuid() };
      fileContainer.uri = `http://redpencil.data.gift/id/dataContainers/${fileContainer.id}`;
      await appendTaskResultFile(task, fileContainer, fileUri);

      await delay(10);


    }

    // make delete delta
    await getTriplesInFileAndApplyByBatch('to-remove-triples.ttl', task, async (tripleStore) => {
      await makeFileFromTriplestore(deletePage => {
        return {
          deletes: deletePage,
          inserts: []
        };
      })(tripleStore);

    });

    // make insert delta
    await getTriplesInFileAndApplyByBatch('new-insert-triples.ttl', task, async (tripleStore) => {
      await makeFileFromTriplestore(insertPage => {
        return {
          deletes: [],
          inserts: insertPage
        };
      })(tripleStore);

    });
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

async function appendTaskResultFile(task, container, fileUri) {
  const queryStr = `
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
    PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    INSERT DATA {
      GRAPH ${sparqlEscapeUri(task.graph)} {
        ${sparqlEscapeUri(container.uri)} a nfo:DataContainer.
        ${sparqlEscapeUri(container.uri)} mu:uuid ${sparqlEscapeString(container.id)}.
        ${sparqlEscapeUri(container.uri)} task:hasFile ${sparqlEscapeUri(fileUri)}.

        ${sparqlEscapeUri(task.task)} task:resultsContainer ${sparqlEscapeUri(container.uri)}.
      }
    }
  `;

  await update(queryStr);

}
