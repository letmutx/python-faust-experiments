import faust

app = faust.App('processor-test', broker='kafka://localhost', topic_partitions=2)

topic1 = app.topic('topic-1', key_type=str, value_type=int)
topic2 = app.topic('topic-2', value_type=str)

table1 = app.Table('table-1', default=lambda: -44, partitions=2)

@app.agent(topic1)
async def processor1(stream):
    async for key, value in stream.items():
        table1[key] = value

@app.agent(topic2)
async def processor2(stream):
    async for key, value in stream.items():
        print('table value:', table1[value])
        yield table1[value]


@app.timer(1.0)
async def timer():
    import random
    await topic1.send(key='magic', value=random.randint(11, 49))
    ret = await processor2.ask(value='magic')
    print(ret)
