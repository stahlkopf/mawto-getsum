import requests
import json
import rethinkdb as r
import sys
import time
import schedule
reload(sys)
sys.setdefaultencoding('utf-8')
RDB_HOST = "159.203.16.47"
RDB_PORT = 28015
RDB_DATABASE = "Mawto"
RDB_AUTHKEY = "atom"
conn = r.connect( RDB_HOST, RDB_PORT, RDB_DATABASE, RDB_AUTHKEY).repl()

def main():
    EXAMPLE_URL = 'http://newspaperjson1.herokuapp.com/article?url='

    cursor = r.db(RDB_DATABASE).table("ArticleURL").filter((r.row["summarizable"]==1)&(r.row["summarized"]==0)).run()
    for document in cursor:
        x=document.get('link')
        url=''.join(x).decode('utf-8')


        try:
            data = requests.get(EXAMPLE_URL+url)
            try:
                data.raise_for_status()
            except Exception as exc:
                r.db(RDB_DATABASE).table('ArticleURL').get(url).update({'summarized': 0}).run()
                r.db(RDB_DATABASE).table('ArticleURL').get(url).update({'summarizable': 0}).run()
                print('There was a problem: %s' % (exc))
                print('Something occured while on %s' % (url))
                cursor.close()
                return 0
            #r.db(RDB_DATABASE).table('ArticleURL').filter({"link" : document['link']}).update({'id': r.uuid(url).run()}).run()
            jdata = data.json()
            jdata['link'] = jdata.pop('url')
            meta = json.dumps(jdata, ensure_ascii=False).encode('utf-8')



            summarymain = {k: v for k, v in jdata.items() if k.startswith('link')}
            summarymain.update({k: v for k, v in jdata.items() if k.startswith('summary1')})
            summarymain.update({k: v for k, v in jdata.items() if k.startswith('summary2')})
            summarymain.update({k: v for k, v in jdata.items() if k.startswith('summary3')})
            summarymain.update({k: v for k, v in jdata.items() if k.startswith('summary4')})
            summarymain.update({k: v for k, v in jdata.items() if k.startswith('summary5')})
            summarymain.update({k: v for k, v in jdata.items() if k.startswith('summary6')})
            summarymain.update({k: v for k, v in jdata.items() if k.startswith('summary7')})
            summarymain.update({k: v for k, v in jdata.items() if k.startswith('title')})
            summarymain.update({k: v for k, v in jdata.items() if k.startswith('meta_description')})
            category = r.db(RDB_DATABASE).table("ArticleGnews").get(url).pluck("category").run()
            sid = r.db(RDB_DATABASE).table("ArticleURL").get(document['link']).pluck("id").run()


            summarybasic = {k: v for k, v in jdata.items() if k.startswith('link')}
            summarybasic.update({k: v for k, v in jdata.items() if k.startswith('summary1')})
            summarybasic.update({k: v for k, v in jdata.items() if k.startswith('summary7')})
            summarybasic.update({k: v for k, v in jdata.items() if k.startswith('title')})
            summarybasic['summaryshort'] = summarybasic.pop('summary1')
            summarybasic['summarylong'] = summarybasic.pop('summary7')
            summarybasic.update({k: v for k, v in jdata.items() if k.startswith('top_image')})
            summarybasic.update({'dateinserted':r.now()})
            summarybasic.update(sid)
            summarybasic.update(category)

            summarymain = json.dumps(summarymain, ensure_ascii=False).encode('utf-8')
            summarymain = json.loads(summarymain)
            summarymain.update({'dateinserted':r.now()})
            summarymain.update(sid)
            summarymain.update(category)

            articlemedia = {k: v for k, v in jdata.items() if k.startswith('link')}
            articlemedia.update({k: v for k, v in jdata.items() if k.startswith('top_image')})
            articlemedia.update({k: v for k, v in jdata.items() if k.startswith('images')})
            articlemedia.update({k: v for k, v in jdata.items() if k.startswith('movies')})
            articlemedia = (json.dumps(articlemedia, ensure_ascii=False).encode('utf-8'))
            articlemedia= (json.loads(articlemedia))
            articlemedia.update(sid)
            articlemedia.update({'dateinserted':r.now()})
            articlemedia.update({'animation':""})


            meta = json.loads(meta)
            meta.update({'dateinserted':r.now()})
            r.db(RDB_DATABASE).table('ArticleMeta').insert(meta).run()
            r.db(RDB_DATABASE).table('ArticleURL').get(url).update({'summarized': 1}).run()
            r.db(RDB_DATABASE).table('ArticleSummaryMain').insert(summarymain).run()
            r.db(RDB_DATABASE).table('ArticleMedia').insert(articlemedia).run()
            r.db(RDB_DATABASE).table('ArticleSummaryBasic').insert(summarybasic).run()
        except requests.exceptions.ConnectionError:
            print ("TIMEOUT REQUEST ERROR ")
            cursor.close()
            return 0

        #print (r.db(RDB_DATABASE).table('URLs').filter({"link" : url}).update({'IsSummarized': 1}))

    cursor.close()


if __name__ == "__main__":
    main()
