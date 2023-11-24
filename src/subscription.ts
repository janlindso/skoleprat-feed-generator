import {
  OutputSchema as RepoEvent,
  isCommit,
} from './lexicon/types/com/atproto/sync/subscribeRepos'
import { FirehoseSubscriptionBase, getOpsByType } from './util/subscription'

export class FirehoseSubscription extends FirehoseSubscriptionBase {
  async handleEvent(evt: RepoEvent) {
    if (!isCommit(evt)) return
    const ops = await getOpsByType(evt)

    const hashtag = '#skole';
    const authors = [];
    const priorityWords = [
      'digitale læremidler', 
      'digitale læremiddel',
      'digital kompetanse', 
      'fagfornyelse',
      'fagfornyelsen',
      'fagfornying',
      'fagfornyinga',
      'kompetansemål',
      'kunnskapsløftet',
      'lk06',
      'læremidler', 
      'læremiddel',
      '#lærerhverdag',
      'pedagogikk', 
      'pedagogisk',  
      'utdanningsforbundet',
      'utdanningsdirektoratet', 
      'vurderingsgrunnlag'
    ];
    const words = [
      '1.trinn',
      '1. trinn',
      '2.trinn',
      '2. trinn',
      '3.trinn',
      '3. trinn',
      '4.trinn',
      '4. trinn',
      '5.trinn',
      '5. trinn',
      '6.trinn',
      '6. trinn',
      '7.trinn',
      '7. trinn',
      '8.trinn',
      '8. trinn',
      '9.trinn',
      '9. trinn',
      '10.trinn',
      '10. trinn',
      'elevene',
      'elevenes', 
      'elever',
      'elevar',
      'elevane',
      'fagtekster',
      'fagprøve',
      'fagbrev',
      'fråværsgrense',
      'fraværsgrensa', 
      'fraværsgrensen', 
      'grunnskolen', 
      'grunnskulen',
      'lærebok', 
      'lærebøker',
      'lærer', 
      'lærar',
      'læring',
      'lærling',
      'lærlinger',
      'lærlingar',
      'lærefag',
      'mobilforbud',
      'skole', 
      'skoler',
      'skolen',
      'skolebøker', 
      'skoledag',
      'skoledagen',
      '#spilliskolen',
      'skule',
      'skulen',
      'skular',
      'skolefrafall', 
      'skulefrafall',
      'studiespesialisering',
      'tonje brenna', 
      'utdanning', 
      'vg1',
      'vg2',
      'vg3',
      'videregående',
      'vidaregåande',
      'yrkesfag',
      'yrkesfagene'
    ];
    const allWords = priorityWords.concat(words);
    const wordInString = (s, word) => new RegExp('\\b' + word + '\\b', 'i').test(s);


    const multiSearchAtLeastN = (text, searchWords, minimumMatches) => {
      let matches = 0;
      for (let word of searchWords) {
        if (wordInString(text, word) && ++matches >= minimumMatches) return true;
      }
      return false;
    };

    const postsToDelete = ops.posts.deletes.map((del) => del.uri)
    const postsToCreate = ops.posts.creates
      .filter((create) => {
        // only skole-related posts
        if (create.record.text.toLowerCase().includes(hashtag)) {
          return true;
        }
        else if (authors.some(el => create.author.toLowerCase().includes(el)) && multiSearchAtLeastN(create.record.text.toLowerCase(), allWords, 1)) {
          return true;
        }
        else if (multiSearchAtLeastN(create.record.text.toLowerCase(), priorityWords, 1) && multiSearchAtLeastN(create.record.text.toLowerCase(), words, 1)) {
          return true;
        }
        else {
          return false;
        }

        
      })
      .map((create) => {
        // map skole-related posts to a db row
        return {
          uri: create.uri,
          cid: create.cid,
          replyParent: create.record?.reply?.parent.uri ?? null,
          replyRoot: create.record?.reply?.root.uri ?? null,
          indexedAt: new Date().toISOString(),
        }
      })

    if (postsToDelete.length > 0) {
      await this.db
        .deleteFrom('post')
        .where('uri', 'in', postsToDelete)
        .execute()
    }
    if (postsToCreate.length > 0) {
      await this.db
        .insertInto('post')
        .values(postsToCreate)
        .onConflict((oc) => oc.doNothing())
        .execute()
    }
  }
}
