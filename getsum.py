import requests
import json
import rethinkdb as r
import sys
import time
import schedule
reload(sys)
sys.setdefaultencoding('utf-8')
RDB_HOST = "testdata.mawto.com"
RDB_PORT = 28015
RDB_DATABASE = "Mawto"
RDB_AUTHKEY = "atom"
conn = r.connect( RDB_HOST, RDB_PORT, RDB_DATABASE, RDB_AUTHKEY).repl()

def main():
    EXAMPLE_URL = 'http://newspaperjson-test.herokuapp.com/article?url='

    cursor = r.db(RDB_DATABASE).table("ArticleStatus").get_all([0, 1], index="todo").run()
    for document in cursor:
        dbid=document.get('id')
        print (dbid)
        url = document.get('link')
        print (url)


        try:
            data = requests.get(EXAMPLE_URL+url)
            try:
                data.raise_for_status()
            except Exception as exc:
                r.db(RDB_DATABASE).table('ArticleStatus').get(dbid).update({'summarized': 0}).run()
                r.db(RDB_DATABASE).table('ArticleStatus').get(dbid).update({'summarizable': 0}).run()
                print('There was a problem: %s' % (exc))
                print('Something occured while on %s' % (url))
                print (data)
                cursor.close()
                return 0
            #r.db(RDB_DATABASE).table('ArticleStatus').filter({"link" : document['link']}).update({'id': r.uuid(url).run()}).run()
            jdata = data.json()
            jdata['link'] = jdata.pop('url')
            meta = json.dumps(jdata, ensure_ascii=False).encode('utf-8')



            summarymain = {k: v for k, v in jdata.items() if k.startswith('link')}
            summarymain.update({k: v for k, v in jdata.items() if k.startswith('summary1')})
            summarymain['summaryshort'] = summarymain.pop('summary1')
            summarymain.update({k: v for k, v in jdata.items() if k.startswith('summary1')})
            summarymain.update({k: v for k, v in jdata.items() if k.startswith('summary2')})
            summarymain.update({k: v for k, v in jdata.items() if k.startswith('summary3')})
            summarymain.update({k: v for k, v in jdata.items() if k.startswith('summary4')})
            summarymain.update({k: v for k, v in jdata.items() if k.startswith('summary5')})
            summarymain.update({k: v for k, v in jdata.items() if k.startswith('summary6')})
            summarymain.update({k: v for k, v in jdata.items() if k.startswith('summary7')})
            summarymain['summarylong'] = summarymain.pop('summary7')
            summarymain.update({k: v for k, v in jdata.items() if k.startswith('summary7')})
            summarymain.update({k: v for k, v in jdata.items() if k.startswith('title')})
            summarymain.update({k: v for k, v in jdata.items() if k.startswith('meta_description')})
            category = r.db(RDB_DATABASE).table("ArticleGnews").get(dbid).pluck("category").run()
            summarymain.update({'dateinserted':r.now()})
            summarymain.update({'id': dbid})
            summarymain.update(category)



            summarybasic = {k: v for k, v in jdata.items() if k.startswith('link')}
            summarybasic.update({k: v for k, v in jdata.items() if k.startswith('summary1')})
            summarybasic.update({k: v for k, v in jdata.items() if k.startswith('summary7')})
            summarybasic.update({k: v for k, v in jdata.items() if k.startswith('title')})
            summarybasic['summaryshort'] = summarybasic.pop('summary1')
            summarybasic['summarylong'] = summarybasic.pop('summary7')
            summarybasic.update({k: v for k, v in jdata.items() if k.startswith('top_image')})
            summarybasic.update({'dateinserted':r.now()})
            summarybasic.update({'id': dbid})
            summarybasic.update({k: v for k, v in jdata.items() if k.startswith('meta_description')})
            summarybasic.update(category)



            articlemedia = {k: v for k, v in jdata.items() if k.startswith('link')}
            articlemedia.update({k: v for k, v in jdata.items() if k.startswith('top_image')})
            articlemedia.update({k: v for k, v in jdata.items() if k.startswith('images')})
            articlemedia.update({k: v for k, v in jdata.items() if k.startswith('movies')})
            articlemedia.update({'id': dbid})
            articlemedia.update({'dateinserted':r.now()})
            articlemedia.update({'animation':""})


            meta = json.loads(meta)
            meta.update({'dateinserted':r.now()})
            meta.update({'id': dbid})
            r.db(RDB_DATABASE).table('ArticleMeta').insert(meta).run()
            r.db(RDB_DATABASE).table('ArticleStatus').get(dbid).update({'summarized': 1}).run()
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
