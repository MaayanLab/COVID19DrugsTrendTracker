library(readr)
library(ggplot2)
library(sqldf)
library(anytime)
library(data.table)
library(dplyr)

PTH = '.../covid/' # set covid directory path
  
# help function
plotme<-function(df,xl='',day){
  p<-ggplot(data=df, aes(x=reorder(DrugSymbol,-freq), y=freq)) +
    labs(title=paste0("Most Tweeted Drugs for COVID-19 on ",day), caption = "Tweetes containing keyword variations of Covid-19 and SARS") +
    labs(x=xl, y="Frequency") +
    geom_bar(stat="identity") +
    geom_text(aes(label=freq), vjust=0, color="black", size=1.5) +
    theme(axis.text.x = element_text(angle = 75, hjust = 1),text = element_text(size=5))
    ggsave(paste0("/home/maayanlab/enrichrbot/covid/data/output/COVID19DrugsTrendTracker/daily_barplots/",day,".png"),width = 8, height = 6, units = "cm")
    write.table(day,file=paste0(PTH,'data/today_barplot.txt'),row.names=F, col.names=F, sep=",")
  #return(p)
}

removelist<-c('nhc','(s,r)-fidarestat','ethanol','fit','an-9','w-9','w-5','w-12','cdc',	'oxygen','w-13','t-2000','tmr','yo-2','selenium','rita','cocaine','h-9','at-001',
              'w 12','Poly-I:CLC','Y-134','SAL-1','ITE','PAC-1','Morphine','NM-3','PPT','s 23', 'T-62','Fica','GET-73','PP-2','PP-3','Xenon','DPN','ML-7','SC-10','obaa',
              'TAS-116','SANT-1','Silver','heat','Nicotine','PPL-100','PIT','Ozone','sucrose','Copper','oxygen','an-9','cdc','fit','w-5','w-12','testosterone','atp',
              'platinum','bia','melatonin','sc-9','neon','acetate','serotonin','chlorine','chlorine-dioxide','acrylic','hydrogen-peroxide','nitric-oxide','saccharin'
              ,'psicose','propylene glycol','cobalt','silicon','asiatic','hypochlorite','nadh','neca','nitrous','citric','chloroform','carfentanil','iron','cobalt',
              'carbon monoxide','carbon monoxide','carbon-monoxide','psilocybin','hydrogen peroxide','titanium','ammonium','chloride','caffeine','calcium','cholesterol',
              'cortisone','donu','alcohol','lactose','aluminium','cccp','serine','piperinem','xylitol','nitric oxide','ammonium chloride','glucosamine',
              'glycerin','histamine','magnesium','mebendazol','namn','n3','thymine','iodine','nitrate','fccp','bntx')


folder<-read_csv(paste0(PTH,"data/log.txt"), col_names = FALSE)$X1

day<-anytime(folder*86400)
day<-format(day, '%Y-%m-%d')
day<-as.Date(day)-1

filepath = paste0(PTH,'data/tweets/',folder,'/full_data.csv.gz')
full_data_csv <- fread(filepath,colClasses = 'character')

# take only tweets from yesterday
x<-full_data_csv
x$tweet_created_at<-as.POSIXct(x$tweet_created_at, format="%a %b %d %H:%M:%S +0000 %Y", tz="GMT")
x$tweet_created_at<-as.Date(x$tweet_created_at, tz = "GMT", format="%m-%d-%Y")
x<-x[x$tweet_created_at<= day,] # keep only tweets for yesterday

day<-format(day, "%m-%d-%Y")

x$DrugSymbol<-tolower(x$DrugSymbol)
x$text<-tolower(x$text)
x$hashtags<-tolower(x$hashtags)
y<-x[grepl("covid|sars|coronavirus",x$text) | grepl("covid|sars|coronavirus",x$hashtags),]
y[y$DrugSymbol=='lopinavir-ritonavir.json.gz','DrugSymbol']<-'lopinavir'
y$len<-nchar(y$DrugSymbol)
y<-y[y$len<31,]
y<-y[!grepl("zinc|Colchiceine|testosterone|vitamin|alcohol|carbon|acid|methylene",y$DrugSymbol),]
y<-y[!grepl(")",y$DrugSymbol),]
y$DrugSymbol<-gsub('"',"",y$DrugSymbol)
y$DrugSymbol<-gsub(".json.gz","",y$DrugSymbol)
y<-y[!y$DrugSymbol %in% tolower(removelist),]

only_tweetids<-y[y$tweet_type=='TW',]$tweet_id
only_tweetids<-unique(only_tweetids)
write.table(only_tweetids,file=paste0(PTH,'/data/output/COVID19DrugsTrendTracker/tweets/',day,'.csv'),row.names=F, col.names=F, sep=",")
rm(only_tweetids)

res<-sqldf("select DrugSymbol, count(1) as freq from y group by DrugSymbol")
res<-res[!is.na(res$DrugSymbol),]
res<-data.frame(res)
res<-res[order(-res$freq),]

names(res)<-c("DrugSymbol","freq")
tmp<-top_n(res, 20, res$freq)
plotme(tmp,xl='',day)

names(res)<-c("Drug",as.character(day))
write.csv(res,file=paste0(PTH,'/data/output/COVID19DrugsTrendTracker/daily_reports/',day,'.csv'),row.names = FALSE)

# read file and add new column
f = paste0(PTH,'data/output/COVID19DrugsTrendTracker/trends.csv')
if(!file.exists(f)){
  write.csv(res,file=f,row.names = FALSE)
}else{
  trends <- read_csv(f)
  if(names(res)[2] %in% names(trends)){
    trends[,names(res)[2]]<-NULL
    print("data deleted from trends")
  }else{
    trends <- merge(trends, res, all = T, by ='Drug')
    trends[is.na(trends)] <- 0
    write.csv(trends,file=f,row.names = FALSE)
  }
}
