class Orca(object):
  '''
  Sample :
  >>> data.show()
  +----------+
  |simpleDate|
  +----------+
  |2004/10/02|
  |2013/08/30|
  +----------+
  
  >>> od = Orca(data.select('OrderDate'))
  >>> od.convertDate('simpleDate','y')\
  .convertDate('simpleDate','addDays',addDays=1)\
  .convertDate('simpleDate','monthFirst',dformat='%Y%m%d',getMonthFirst=True)\
  .convertDate('simpleDate','monthLast',getMonthLast=True)\
  .convertDate('simpleDate','replaceBy',replaceBy=15,dformat='%Y/%m/%d').toSpark().show()
  
  +----------+----------+----------+----------+----------+----------+
  |simpleDate|         y|   addDays|monthFirst| monthLast| replaceBy|
  +----------+----------+----------+----------+----------+----------+
  |2004/10/02|2004-10-02|2004-10-03|  20041001|2004-10-31|2004/10/15|
  |2013/08/30|2013-08-30|2013-08-31|  20130801|2013-08-31|2013/08/15|
  +----------+----------+----------+----------+----------+----------+
  
  '''
    def __init__(self,df):
        self.df = df
    def toSpark(self):
        return self.df
    def convertDate(self,
                    column:str,
                    newName:str=None
                ,dformat:str='%Y-%m-%d',
                addDays=0,
                getMonthFirst:bool=False,
                getMonthLast:bool=False,
                replaceBy:int=None,
                vector:bool=True)->DataFrame:
    
        def _(dt:Any)->str:
            if isinstance(dt,str):
                converted = dateutil.parser.parse(dt)
            elif isinstance(dt,datetime.datetime):
                converted = dt
            elif isinstance(dt,(int,float)):
                converted = dateutil.parser.parse(str(int(dt)))
            else:
                return 'Unrecognized format'
            if getMonthFirst:
                converted = converted.replace(day=1)
            if getMonthLast:
                next_month = converted.replace(day=28) + datetime.timedelta(days=4)
                converted  = next_month - datetime.timedelta(days=next_month.day)
            if replaceBy:
                converted = converted.replace(day=replaceBy)
            converted = converted+datetime.timedelta(days=addDays)
            return converted.strftime(dformat)

        def covertDatePandas(df:pd.Series)->pd.Series:
            return df.apply(_)

        converterVector = pandas_udf(covertDatePandas,returnType=StringType())
        converterScala  = udf(_,returnType=StringType()) 

        reqUDF = converterVector if vector else converterScala
        name   = column if newName is None else newName
        return Orca(self.df.withColumn(name,reqUDF(col(column))))
    
