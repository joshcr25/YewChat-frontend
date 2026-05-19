use yew_agent::Dispatched;
use crate::services::event_bus::{EventBus, Request};
use futures::{channel::mpsc::Sender, SinkExt, StreamExt};
use reqwasm::websocket::{futures::WebSocket, Message};

use wasm_bindgen_futures::spawn_local;

pub struct WebsocketService {
    pub tx: Sender<String>,
}

// ... (kode impor lainnya tidak berubah)

impl WebsocketService {
    pub fn new() -> Self {
        // Gunakan port ini ke port backend- nya (misalnya: 8080)
        // Jangan gunakan 8080 karena sudah dipakai oleh front end (Trunk)
        let ws = WebSocket::open("ws://127.0.0.1:8080").expect("Gagal membuka WebSocket");

        let (mut write, mut read) = ws.split();
        let (in_tx, mut in_rx) = futures::channel::mpsc::channel::<String>(1000);
        let mut event_bus = EventBus::dispatcher();

        spawn_local(async move {
            while let Some(s) = in_rx.next().await {
                log::debug!("got event from channel! {}", s);
                if let Err(e) = write.send(Message::Text(s)).await {
                    log::error!("Error saat mengirim ke websocket: {:?}", e);
                }
            }
        });

        spawn_local(async move {
            while let Some(msg) = read.next().await {
                match msg {
                    Ok(Message::Text(data)) => {
                        log::debug!("from websocket: {}", data);
                        event_bus.send(Request::EventBusMsg(data));
                    }
                    Ok(Message::Bytes(b)) => {
                        if let Ok(val) = std::str::from_utf8(&b) {
                            log::debug!("from websocket: {}", val);
                            event_bus.send(Request::EventBusMsg(val.into()));
                        }
                    }
                    Err(e) => {
                        log::error!("ws error: {:?}", e)
                    }
                }
            }
            log::debug!("WebSocket Closed");
        });

        Self { tx: in_tx }
    }
}