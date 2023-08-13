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
    const words = [

      'læring', 
      'pedagogikk', 
      'pedagogisk', 
      'utdanning', 
      'skole', 
      'videregående', 
      'grunnskolen', 
      'tonje brenna', 
      'utdanningsforbundet',
      'utdanningsdirektoratet', 
      'læremidler', 
      'digitale læremidler', 
      'digital kompetanse', 
      'skolefrafall', 
      'fraværsgrensa', 
      'fraværsgrensen', 
      'skolebøker', 
      'læremiddel', 
      'lærer', 
      'elevene', 
      'elever'
    ];

    const multiSearchAtLeastN = (text, searchWords, minimumMatches) => {
      let matches = 0;
      for (let word of searchWords) {
        if (text.includes(word) && ++matches >= minimumMatches) return true;
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
        else if (authors.some(el => create.author.toLowerCase().includes(el)) && multiSearchAtLeastN(create.record.text.toLowerCase(), words, 1)) {
          return true;
        }
        else if (multiSearchAtLeastN(create.record.text.toLowerCase(), words, 2)) {
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
