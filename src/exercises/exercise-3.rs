use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    select,
    sync::broadcast::{Receiver, Sender, channel},
};

const IP_ADDRESS: &str = "127.0.0.1";
const PORT: u16 = 8080;
const CAPACITY: usize = 100;

#[derive(Clone)]
struct Message {
    sender: String,
    content: String,
}

#[tokio::main]
async fn main() {
    let (broadcast_tx, _broadcast_receiver): (
        Sender<Message>,
        Receiver<Message>,
    ) = channel(CAPACITY);

    let mut user_id = 1;

    let Ok(listener) = TcpListener::bind(format!("{IP_ADDRESS}:{PORT}")).await
    else {
        eprintln!("Failed to bind {}:{}", IP_ADDRESS, PORT);
        return;
    };

    println!("Chat server running on {}:{}", IP_ADDRESS, PORT);

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                println!("New client connected: {}", addr);

                let mut broadcast_rx = broadcast_tx.subscribe();
                let broadcast_tx = broadcast_tx.clone();

                let next_user_id = user_id;
                user_id += 1;
                let user_name = format!("User{}", next_user_id);

                tokio::spawn(async move {
                    let (rx, mut tx) = stream.into_split();
                    let mut reader = BufReader::new(rx);

                    let join_message = Message {
                        sender: user_name.clone(),
                        content: "has joiner the chart.".to_string(),
                    };

                    if let Err(e) = broadcast_tx.send(join_message) {
                        eprintln!("Failed to broadcast join message: {}", e);
                        return;
                    }

                    loop {
                        let mut buf = String::new();

                        select! {
                          Ok(n) = reader.read_line(&mut buf) => {
                            if n == 0 {
                              let leave_message = Message {
                                sender: user_name.clone(),
                                content: "has left the chat".to_string(),
                              };

                              broadcast_tx.send(leave_message).map_err(
                                |e| eprintln!("Failed to broadcast leave message: {}", e)
                              ).ok();
                              break;
                            }

                            let message = Message {
                              sender: user_name.clone(),
                              content: buf.trim().to_string()
                            };

                            if let Err(e) = broadcast_tx.send(message) {
                              eprintln!("Failed to broadcast message: {}", e);
                              break;
                            }
                          }

                          Ok(msg) = broadcast_rx.recv() => {
                            if msg.sender == user_name {
                              continue;
                            }

                            let msg = format!("{}: {}\r\n", msg.sender, msg.content);
                            if let Err(e) = tx.write_all(msg.as_bytes()).await {
                              eprintln!("Failed to send message to {}: {}", user_name, e);
                              break;
                            }
                          }
                        }
                    }
                });
            }
            Err(e) => {
                eprintln!("Failed to accept connection: {}", e);
                continue;
            }
        }
    }
}
