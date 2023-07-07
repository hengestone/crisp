import {onReceive} from "./bars.js"

function ws_subscribe() {
  var host = window.location.host;
  var protocol = window.location.protocol == "https" ? "wss" : "ws";
  var url = protocol + "://" + host + "/ws";
  console.log(url);
  var socket = new ReconnectingWebSocket(url);
  socket.addEventListener("open", ws_connect);
  socket.addEventListener("message", ws_data_event);
  socket.addEventListener("close", ws_disconnect);
  window.ws_socket = socket
  return socket;
}

function ws_connect() {
  console.log("connected");
}

function ws_data_event(message) {
  var msg = JSON.parse(message.data);
  console.log(msg);
  if (msg.event.action == "counts") {
    var counts = msg.counts;
    var count_nums = [
      parseInt(counts.queued),
      parseInt(counts.runnable),
      parseInt(counts.running),
      parseInt(counts.finished),
      parseInt(counts.failed)
    ];

    onReceive(msg.queue, count_nums);
  }
}

function ws_disconnect() {
  console.log("disconnected");
}
console.log("About to connect");
setTimeout(ws_subscribe, 1000);
