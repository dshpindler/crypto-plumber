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
        console.log('running init!!!!!!!');
        this.connection = await Amqp.connect(this.url);
        this.channel = await this.connection.createChannel();
        this.channel.assertQueue(this.queue, { durable: false });
    }

    async getChannel() {
        if (!this.channel) {
            return await this.init();
        }

        return this.channel;
    }

    send(message: string) {
        return this.getChannel()
            .then((channel: Amqp.Channel) => {
                channel.sendToQueue(this.queue, Buffer.from(message));
                console.log(" [x] Sent %s", message);
            })
            .catch(error => {
                console.error(error);
            });
    }
}

