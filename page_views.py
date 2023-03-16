# import faust
#
# app = faust.App(
#     'page_views',
#     broker='kafka://localhost:29092',
#     topic_partitions=4,
# )
#
#
# class PageView(faust.Record):
#     id: str
#     user: str
#
#
# page_view_topic = app.topic('page_views', value_type=PageView)
# page_views = app.Table('page_views', default=int)
#
#
# @app.agent(page_view_topic)
# async def count_page_views(views):
#     async for view in views.group_by(PageView.id):
#         print(view.id, view.user)
#         page_views[view.id] += 1

import faust


app = faust.App('hit_counter',broker="kafka://localhost:29092")

class hitCount(faust.Record,validation=True):
    hits: int
    timestamp: float
    userId: str


hit_topic = app.topic("hit_count",value_type=hitCount)
count_topic = app.topic('count_topic', internal=True, partitions=1, value_type=hitCount)

hits_table = app.Table('hitCount', default=int)
count_table = app.Table("major-count",key_type=str,value_type=int,partitions=1,default=int)

@app.agent(hit_topic)
async def count_hits(counts):
    async for count in counts:
        print(f"Data recieved is {count}")
        if count.hits > 20:
            await count_topic.send(value=count)


@app.agent(count_topic)
async def increment_count(counts):
    async for count in counts:
        print(f"Count in internal topic is {count}")
        count_table[str(count.userId)]+=1
        print(f'{str(count.userId)} has now been seen {count_table[str(count.userId)]} times')
