import * as Amqp from "amqplib";

export class AmqpSender {
    private url: string;
    private queue: string;

    private connection: Amqp.Connection;
    private channel: Amqp.Channel;

    constructor(url: string, queue: string) {
        this.url = url;
        this.queue = queue;
    }

    async init() {
        console.log('running init!!!!!!!')
        await Amqp.connect(this.url)
            .then((connection: Amqp.Connection) => {
                this.connection = connection;
                return connection.createChannel()
            }).then((channel: Amqp.Channel) => {
                this.channel = channel;
                channel.assertQueue(this.queue, { durable: false });
            })
    }

    async getChannel() {
        if (!this.channel) {
            return await this.init();
        }

        return this.channel;
    }

    send(message: string) {
        this.getChannel()
            .then((channel) => {
                this.channel.sendToQueue(this.queue, Buffer.from(message));
                console.log(" [x] Sent %s", message);
            })
            .catch(error => {
                console.error(error);
                // this.connection.close();
                // this.channel.close();
                // this.channel = null;
            });
    }
}

