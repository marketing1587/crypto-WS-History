console.log("hello")

var methods = {}
methods.ws = function () {
var db, db_5min, db_10min, db_15min, db_30min, db_1h, db_4h, db_24h

mongodb.MongoClient.connect('mongodb://ortal:Ortal1234@ds129560.mlab.com:29560/cexio_historique_5min', function (err, database) {
    if (err) {
        console.log(err);
        process.exit(1);
    }
    db_5min = database.db("cexio_historique_5min")
})

mongodb.MongoClient.connect('mongodb://ortal:Ortal1234@ds129560.mlab.com:29560/cexio_historique_10min', function (err, database) {
    if (err) {
        console.log(err);
        process.exit(1);
    }
    db_10min = database.db("cexio_historique_10min")
})

mongodb.MongoClient.connect('mongodb://ortal:Ortal1234@ds129560.mlab.com:29560/cexio_historique_15min', function (err, database) {
    if (err) {
        console.log(err);
        process.exit(1);
    }
    db_15min = database.db("cexio_historique_15min")
})

mongodb.MongoClient.connect('mongodb://ortal:Ortal1234@ds129560.mlab.com:29560/cexio_historique_30min', function (err, database) {
    if (err) {
        console.log(err);
        process.exit(1);
    }
    db_30min = database.db("cexio_historique_30min")
})

mongodb.MongoClient.connect('mongodb://ortal:Ortal1234@ds129560.mlab.com:29560/cexio_historique_1h', function (err, database) {
    if (err) {
        console.log(err);
        process.exit(1);
    }
    db_1h = database.db("cexio_historique_1h")
})

mongodb.MongoClient.connect('mongodb://ortal:Ortal1234@ds129560.mlab.com:29560/cexio_historique_4h', function (err, database) {
    if (err) {
        console.log(err);
        process.exit(1);
    }
    db_4h = database.db("cexio_historique_4h")
})

mongodb.MongoClient.connect('mongodb://ortal:Ortal1234@ds129560.mlab.com:29560/cexio_historique_24h', function (err, database) {
    if (err) {
        console.log(err);
        process.exit(1);
    }
    db_24h = database.db("cexio_historique_24h")
})

mongodb.MongoClient.connect('mongodb://ortal:Ortal1234@ds117540.mlab.com:17540/cexiohistorique', function (err, database) {
    if (err) {
        console.log(err);
        process.exit(1);
    }
    db = database.db("cexiohistorique")

    console.log("in cexiohistorique")

            let msg_send = JSON.stringify({
                "e": "subscribe",
                "rooms": ["tickers"]
            });
        
            var myobjet = new Object();
            var myobjet_5min = new Object();
            var myobjet_10min = new Object();
            var myobjet_30min = new Object();
            var myobjet_1h = new Object();
            var myobjet_4h = new Object();
            var myobjet_24h = new Object();
            var Keeping = new Object();
            var Keep_high, Keep_low, myHigh

            const w = new ws('wss://ws.cex.io/ws/')
            w.on('open', () => w.send(msg_send))
            w.on('error', function(e){
                console.log("error**",e);
            });
            w.on('message', (msg_received) => {
                console.log("message",msg_received)
                if (msg_received.indexOf("tick") != -1) 
                {   
                    msg_received = JSON.parse(msg_received);
                    
                    //console.log("msg_received " + msg_received.data.symbol1 + "_" + msg_received.data.symbol2 + " " + msg_received.data.price)    
                                  
                    var my_pair = msg_received.data.symbol1 + "_" + msg_received.data.symbol2
                    var mydate = getMyDate(new Date());
                    var myLow, myOpen, myHigh_5min, myLow_5min, myOpen_5min
                    //console.log("myobjet[my_pair]",myobjet[my_pair])
                    if(myobjet[my_pair] != undefined)
                    {   
                        if(myobjet[my_pair].high < msg_received.data.price) {
                        myHigh = msg_received.data.price
                        //console.log("**Second** high",myHigh) 
                        }
                        else { 
                        myHigh = myobjet[my_pair].high
                        //console.log("**Second** high",myHigh)
                        }

                        if(myobjet[my_pair].low > msg_received.data.price)  
                        {myLow = msg_received.data.price
                        //console.log("**Second** low",myLow)
                        }
                        else { myLow = myobjet[my_pair].low
                            //console.log("**Second** low",myLow)
                        }

                        myOpen = myobjet[my_pair].open
                        myobjet[my_pair] = {exchange: "CEX", price: msg_received.data.price, high: myHigh , low: myLow , open: myOpen, close: msg_received.data.price, open24: msg_received.data.open24, 
                        volume: msg_received.data.volume, date: mydate}
                    }
                    else
                    {   //console.log("object out")
                        myobjet[my_pair] = {exchange: "CEX", price: msg_received.data.price, high: msg_received.data.price , low: msg_received.data.price , open: msg_received.data.price, close: msg_received.data.price,
                        open24: msg_received.data.open24, volume: msg_received.data.volume, date: mydate}
                    }

                    // if(myobjet_5min[my_pair] != undefined)
                    // {   console.log("object in")
                    //     if(myobjet_5min[my_pair].high < msg_received.data.price) {myHigh_5min = msg_received.data.price}
                    //     else { myHigh_5min = myobjet_5min[my_pair].high}

                    //     if(myobjet_5min[my_pair].low > msg_received.data.price)  {myLow_5min = msg_received.data.price}
                    //     else { myLow_5min = myobjet_5min[my_pair].low}

                    //     myOpen_5min = myobjet_5min[my_pair].open
                    //     myobjet_5min[my_pair] = {exchange: "CEX", price: msg_received.data.price, high: myHigh_5min , low: myLow_5min , open: myOpen_5min, close: msg_received.data.price, open24: msg_received.data.open24, 
                    //     volume: msg_received.data.volume, date: mydate}
                    // }
                    // else
                    // {   console.log("object out")
                    //     myobjet_5min[my_pair] = {exchange: "CEX", price: msg_received.data.price, high: msg_received.data.price , low: msg_received.data.price , open: msg_received.data.price, close: msg_received.data.price,
                    //     open24: msg_received.data.open24, volume: msg_received.data.volume, date: mydate}
                    // }
                    //console.log("myobjet",myobjet)
                    //console.log("myobjet _5min",myobjet_5min)
                }
            })

            //1 min
            var i = 0;
            setInterval(function () {
               for (property in myobjet) {
                var date_now = new Date();
                var my_historique_date = getMyDate(date_now);
                myobjet[property].date = my_historique_date;
                var my_previous_date = get_1min_ago_Date(date_now);     
                insertToDB(property, my_previous_date, myobjet, Keeping)
               }
                i++;
                console.log("CEX  i " + i + " date: " + my_historique_date.split(" ")[1])              
            }, 1000 * 60 * 1);

            //5 min
            var i_5 = 0;            
            // setInterval(function () {
            //    for (property in myobjet_5min) {
            //     var date_now_5 = new Date();
            //     var my_historique_date_5 = getMyDate(date_now_5);
            //     myobjet_5min[property].date = my_historique_date_5;
            //     var my_previous_date_5 = get_5min_ago_Date(date_now_5);
            //     insertToDB5min(property, my_previous_date_5, myobjet_5min)
            //     }
            //     i_5++;
            //     console.log("CEX  i_5 " + i_5 + " date: " + my_historique_date_5.split(" ")[1])
            // },1000 * 60 * 5);

            //10 min
            var i_10 = 0;      
            // setInterval(function () {
            //    for (property in myobjet) {
            //     date_now_10 = new Date();
            //     my_historique_date_10 = getMyDate(date_now_10);
            //     myobjet[property].date = my_historique_date_10;
            //     my_previous_date_10 = get_10min_ago_Date(date_now_10);
            //     insertToDB10min(property, my_previous_date_10, myobjet)
            //     }
            //     i_10++;
            //     console.log("CEX  i_10 " + i_10 + " date: " + my_historique_date.split(" ")[1])
            // }, 1000 * 60 * 10); 
            
             //15 min
             var i_15 = 0;      
            //  setInterval(function () {
            //     for (property in myobjet) {
            //      date_now_15 = new Date();
            //      my_historique_date_15 = getMyDate(date_now_15);
            //      myobjet[property].date = my_historique_date_15;
            //      my_previous_date_15 = get_15min_ago_Date(date_now_15);
            //      insertToDB15min(property, my_previous_date_15, myobjet)
            //      }
            //      i_15++;
            //      console.log("CEX  i_15 " + i_15 + " date: " + my_historique_date.split(" ")[1])
            // }, 1000 * 60 * 15);  

             //30 min
             var i_30 = 0;      
            //  setInterval(function () {
            //     for (property in myobjet) {
            //      date_now_30 = new Date();
            //      my_historique_date_30 = getMyDate(date_now_30);
            //      myobjet[property].date = my_historique_date_30;
            //      my_previous_date_30 = get_30min_ago_Date(date_now_30);
            //      insertToDB30min(property, my_previous_date_30, myobjet)
            //      }
            //      i_30++;
            //      console.log("CEX  i_30 " + i_30 + " date: " + my_historique_date.split(" ")[1])
            // }, 1000 * 60 * 30);  
            
             //1h
             var i_1h = 0;      
            //  setInterval(function () {
            //     for (property in myobjet) {
            //      date_now_1h = new Date();
            //      my_historique_date_1h = getMyDate(date_now_1h);
            //      myobjet[property].date = my_historique_date_1h;
            //      my_previous_date_1h = get_1h_ago_Date(date_now_1h);
            //      insertToDB1h(property, my_previous_date_1h, myobjet)
            //      }
            //      i_1h++;
            //      console.log("CEX  i_1h " + i_1h + " date: " + my_historique_date.split(" ")[1])
            // }, 1000 * 60 * 60);  

             //4h
             var i_4h = 0;      
            //  setInterval(function () {
            //     for (property in myobjet) {
            //      date_now_4h = new Date();
            //      my_historique_date_4h = getMyDate(date_now_4h);
            //      myobjet[property].date = my_historique_date_4h;
            //      my_previous_date_4h = get_4h_ago_Date(date_now_4h);
            //      insertToDB4h(property, my_previous_date_4h, myobjet)
            //      }
            //      i_4h++;
            //      console.log("CEX  i_4h " + i_4h + " date: " + my_historique_date.split(" ")[2])
            //  }, 1000 * 60 * 60 * 4);  

             //24h
             var i_24h = 0;      
            //  setInterval(function () {
            //     for (property in myobjet) {
            //      date_now_24h = new Date();
            //      my_historique_date_24h = getMyDate(date_now_24h);
            //      myobjet[property].date = my_historique_date_24h;
            //      my_previous_date_24h = get_24h_ago_Date(date_now_24h);
            //      insertToDB24h(property, my_previous_date_24h, myobjet)
            //      }
            //      i_24h++;
            //      console.log("CEX  i_24h " + i_24h + " date: " + my_historique_date.split(" ")[2])
            //  }, 1000 * 60 * 60 * 24);
         
})


// 1min
function insertToDB(property, my_previous_date, myobjet,Keeping){
    var object_to_send = {};
    if(myobjet[property].high == 0) 
    {  
        myobjet[property].high = Keeping[property].high
        //console.log("High was 0 and now: " + myobjet[property].high)
    }
    if(myobjet[property].low == Number.POSITIVE_INFINITY) 
    { 
        myobjet[property].low = Keeping[property].low
        //console.log("Low was 0 and now: " + myobjet[property].low)
    }
    db.collection(property).count(function (err, count) {
        if (!err && count === 0) {
            for (property2 in myobjet[property])
            { object_to_send[property2] = myobjet[property][property2] }
            db.collection(property).insert(object_to_send, function(err, records){
                //console.log(property + " before***  high: " + myobjet[property].high + " low: "+ myobjet[property].high)
                //console.log("Keeping",Keeping)
                Keeping[property] = {'high': myobjet[property].high, 'low': myobjet[property].low}
                myobjet[property].high = 0
                myobjet[property].low = Number.POSITIVE_INFINITY
                myobjet[property].open = myobjet[property].close
                //console.log(property + " after***  high: " + myobjet[property].high + " low: "+ myobjet[property].high)
            })
        }
        else{
            db.collection(property).find().sort({_id:-1}).limit(1).toArray(function(err, result) { 
                if (err) throw err;
                var the_last_date = result[0].date
                if (the_last_date != my_previous_date)
                {   
                    console.log(property + " db_not equals*** In_table " + the_last_date + " my_previous_date " + my_previous_date)
                }
                for (property2 in myobjet[property])
                { object_to_send[property2] = myobjet[property][property2] }
                db.collection(property).insert(object_to_send, function(err, records){
                    //console.log(property + " before***  high: " + myobjet[property].high + " low: "+ myobjet[property].high)
                    //console.log("Keeping",Keeping)
                    Keeping[property] = {'high': myobjet[property].high, 'low': myobjet[property].low}
                    myobjet[property].high = 0
                    myobjet[property].low = Number.POSITIVE_INFINITY
                    myobjet[property].open = myobjet[property].price
                    //console.log(property + " after***  high: " + myobjet[property].high + " low: "+ myobjet[property].high)
                })
             });

            }
    });
}

// 5min
function insertToDB5min(property, my_previous_date_5, myobjet_5min){
    var object_to_send = {};
    db_5min.collection(property).count(function (err, count) {
        if (!err && count === 0) {
            for (property2 in myobjet_5min[property])
            { object_to_send[property2] = myobjet_5min[property][property2] }
            db_5min.collection(property).insert(object_to_send)  
            myobjet_5min[property].high = 0
            myobjet_5min[property].low = Number.POSITIVE_INFINITY
            myobjet_5min[property].open = myobjet_5min[property].close
        }  
        else{
            db_5min.collection(property).find().sort({_id:-1}).limit(1).toArray(function(err, result) { 
                if (err) throw err;
                var the_last_date_5 = result[0].date
                if (the_last_date_5 != my_previous_date_5)
                {  
                    console.log(property + " db_5min_not equals*** In_table " + the_last_date_5 + " my_previous_date " + my_previous_date_5) 
                }
               
                for (property2 in myobjet_5min[property])
                { object_to_send[property2] = myobjet_5min[property][property2] }
                db.collection(property).insert(object_to_send, function(err, records){
                    myobjet[property].high = 0
                    myobjet[property].low = Number.POSITIVE_INFINITY
                    myobjet[property].open = myobjet[property].close
                })
                
            });
            
        }
    });
}

// 10min
function insertToDB10min(property, my_previous_date, myobjet){
    var object_to_send = {};
    db_10min.collection(property).count(function (err, count) {
        if (!err && count === 0) {
            for (property2 in myobjet[property])
            { object_to_send[property2] = myobjet[property][property2] }
            db_10min.collection(property).insert(object_to_send)  
        }
        else{
            db_10min.collection(property).find().sort({_id:-1}).limit(1).toArray(function(err, result) { 
                if (err) throw err;
                var the_last_date = result[0].date
                if (the_last_date != my_previous_date)
                {  
                    console.log("db_10min_not equals**************last_date_in_table " + the_last_date + " my_previous_date " + my_previous_date) 
                }
               
                for (property2 in myobjet[property])
                { object_to_send[property2] = myobjet[property][property2] }
                db_10min.collection(property).insert(object_to_send)
                
            });
            
        }
    });
}

// 15min
function insertToDB15min(property,my_previous_date,myobjet){
    var object_to_send = {};
    db_15min.collection(property).count(function (err, count) {
        if (!err && count === 0) {
            for (property2 in myobjet[property])
            { object_to_send[property2] = myobjet[property][property2] }
            db_15min.collection(property).insert(object_to_send)  
        }
        else{
            db_15min.collection(property).find().sort({_id:-1}).limit(1).toArray(function(err, result) { 
                if (err) throw err;
                var the_last_date = result[0].date
                if (the_last_date != my_previous_date)
                {  
                    console.log("db_15min_not equals**************last_date_in_table " + the_last_date + " my_previous_date " + my_previous_date) 
                }
               
                for (property2 in myobjet[property])
                { object_to_send[property2] = myobjet[property][property2] }
                db_15min.collection(property).insert(object_to_send)
                
            });
            
        }
    });
}

// 30min
function insertToDB30min(property,my_previous_date,myobjet){
    var object_to_send = {};
    db_30min.collection(property).count(function (err, count) {
        if (!err && count === 0) {
            for (property2 in myobjet[property])
            { object_to_send[property2] = myobjet[property][property2] }
            db_30min.collection(property).insert(object_to_send)  
        }
        else{
            db_30min.collection(property).find().sort({_id:-1}).limit(1).toArray(function(err, result) { 
                if (err) throw err;
                var the_last_date = result[0].date
                if (the_last_date != my_previous_date)
                {  
                    console.log("db_30min_not equals**************last_date_in_table " + the_last_date + " my_previous_date " + my_previous_date) 
                }
               
                for (property2 in myobjet[property])
                { object_to_send[property2] = myobjet[property][property2] }
                db_30min.collection(property).insert(object_to_send)
                
            });
            
        }
    });
}

// 1h
function insertToDB1h(property,my_previous_date,myobjet){
    var object_to_send = {};
    db_1h.collection(property).count(function (err, count) {
        if (!err && count === 0) {
            for (property2 in myobjet[property])
            { object_to_send[property2] = myobjet[property][property2] }
            db_1h.collection(property).insert(object_to_send)  
        }
        else{
            db_1h.collection(property).find().sort({_id:-1}).limit(1).toArray(function(err, result) { 
                if (err) throw err;
                var the_last_date = result[0].date
                if (the_last_date != my_previous_date)
                {  
                    console.log("db_1h_not equals**************last_date_in_table " + the_last_date + " my_previous_date " + my_previous_date) 
                }
               
                for (property2 in myobjet[property])
                { object_to_send[property2] = myobjet[property][property2] }
                db_1h.collection(property).insert(object_to_send)
                
            });
            
        }
    });
}

// 4h
function insertToDB4h(property,my_previous_date,myobjet){
    var object_to_send = {};
    db_4h.collection(property).count(function (err, count) {
        if (!err && count === 0) {
            for (property2 in myobjet[property])
            { object_to_send[property2] = myobjet[property][property2] }
            db_4h.collection(property).insert(object_to_send)  
        }
        else{
            db_4h.collection(property).find().sort({_id:-1}).limit(1).toArray(function(err, result) { 
                if (err) throw err;
                var the_last_date = result[0].date
                if (the_last_date != my_previous_date)
                {  
                    console.log("db_4h_not equals**************last_date_in_table " + the_last_date + " my_previous_date " + my_previous_date) 
                }
               
                for (property2 in myobjet[property])
                { object_to_send[property2] = myobjet[property][property2] }
                db_4h.collection(property).insert(object_to_send)
                
            });
            
        }
    });
}

// 24h
function insertToDB24h(property,my_previous_date,myobjet){
    var object_to_send = {};
    db_24h.collection(property).count(function (err, count) {
        if (!err && count === 0) {
            for (property2 in myobjet[property])
            { object_to_send[property2] = myobjet[property][property2] }
            db_24h.collection(property).insert(object_to_send)  
        }
        else{
            db_24h.collection(property).find().sort({_id:-1}).limit(1).toArray(function(err, result) { 
                if (err) throw err;
                var the_last_date = result[0].date
                if (the_last_date != my_previous_date)
                {  
                    console.log("db_24h_not equals**************last_date_in_table " + the_last_date + " my_previous_date " + my_previous_date) 
                }
               
                for (property2 in myobjet[property])
                { object_to_send[property2] = myobjet[property][property2] }
                db_24h.collection(property).insert(object_to_send)
                
            });
            
        }
    });
}


function getMyDate(dateObj)
{ var year = dateObj.getFullYear();
  var month = ('0' + (dateObj.getMonth() + 1)).slice(-2);
  var day = ('0' + dateObj.getDate()).slice(-2);
  var shortDate = year + '-' + month + '-' + day;
  var Hours = dateObj.getHours()
  var Minutes = ('0' + dateObj.getMinutes()).slice(-2);
  var Seconde = dateObj.getSeconds();
  var shortTime = Hours + ':' + Minutes + ':00'
  var my_Final_Date = shortDate + ' ' + shortTime
  return my_Final_Date
}



function get_1min_ago_Date(dateObj)
{ 
    var MS_PER_MINUTE = 60000;
    var myPreviousDate = new Date(dateObj - 1 * MS_PER_MINUTE);
    myPreviousDate = getMyDate(myPreviousDate)
    return myPreviousDate
}

function get_5min_ago_Date(dateObj)
{ 
    var MS_PER_MINUTE = 60000;
    var myPreviousDate = new Date(dateObj - 5 * MS_PER_MINUTE);
    myPreviousDate = getMyDate(myPreviousDate)
    return myPreviousDate
}

function get_10min_ago_Date(dateObj)
{ 
    var MS_PER_MINUTE = 60000;
    var myPreviousDate = new Date(dateObj - 10 * MS_PER_MINUTE);
    myPreviousDate = getMyDate(myPreviousDate)
    return myPreviousDate
}

function get_15min_ago_Date(dateObj)
{ 
    var MS_PER_MINUTE = 60000;
    var myPreviousDate = new Date(dateObj - 15 * MS_PER_MINUTE);
    myPreviousDate = getMyDate(myPreviousDate)
    return myPreviousDate
}

function get_30min_ago_Date(dateObj)
{ 
    var MS_PER_MINUTE = 60000;
    var myPreviousDate = new Date(dateObj - 30 * MS_PER_MINUTE);
    myPreviousDate = getMyDate(myPreviousDate)
    return myPreviousDate
}

function get_1h_ago_Date(dateObj)
{ 
    var MS_PER_MINUTE = 60000;
    var myPreviousDate = new Date(dateObj - 60 * MS_PER_MINUTE);
    myPreviousDate = getMyDate(myPreviousDate)
    return myPreviousDate
}

function get_4h_ago_Date(dateObj)
{ 
    var MS_PER_MINUTE = 60000;
    var temp = 60 * 4
    var myPreviousDate = new Date(dateObj - temp * MS_PER_MINUTE);
    myPreviousDate = getMyDate(myPreviousDate)
    return myPreviousDate
}

function get_24h_ago_Date(dateObj)
{ 
    var MS_PER_MINUTE = 60000;
    var temp = 60 * 24
    var myPreviousDate = new Date(dateObj - temp * MS_PER_MINUTE);
    myPreviousDate = getMyDate(myPreviousDate)
    return myPreviousDate
}


}
exports.data = methods;