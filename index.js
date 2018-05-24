path = require('path')
http = require('http');
app = require('express')();
server = http.createServer(app);
io = require('socket.io')(server);  
server.listen(3000);
mongodb = require("mongodb");
cors = require('cors');
ws = require('ws')
Pusher = require('pusher-js')
CoinbaseExchange = require('gdax');
coinbase = require('coinbase');
Poloniex = require('poloniex-api-node');
y=require('events').EventEmitter.prototype._maxListeners = 100;
app.use(cors()); 
cex_export = require('./CexIo_Historique.js')
bitstamp_export = require("./Bitstamp_Historique.js")
bitfinex_export = require('./Bitfinex_Historique.js')
poloniex_export = require("./Poloniex_Historique.js")
coinbase_export = require('./Coinbase_Historique.js')

io.sockets.on('connection', function (socket) {   
   console.log("client connected to server")
   socket.on('room', function(room) {
       console.log("client wants to connect to room", room)
       if(typeof room == "string")
       {
           console.log("client connected to room", room)
           socket.join(room);
       }
       else
       {
           room.forEach(i => {
               console.log("client connected to room",i)
               socket.join(i);
           }
           );  
       }
     });


   })


//    bitstamp_export.data.ws()
//    bitfinex_export.data.ws()
//    poloniex_export.data.ws()
   cex_export.data.ws()
//    coinbase_export.data.ws()