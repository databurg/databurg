import * as tls from 'tls';
import * as crypto from 'crypto';
import { EventEmitter } from 'events';

const REQUEST_TIMEOUT_MS = 3000;
const MESSAGE_LENGTH_BYTES = 4;

interface TLSClientOptions {
    host: string;
    port: number;
    /**
     * SHA-256 of the server's TLS certificate, 64 hex characters — the same pin
     * the Rust client verifies via SERVER_CERT_SHA256. The server presents a
     * self-signed certificate, so this pin, not a CA chain, is what establishes
     * trust. Required: without it the connection is refused rather than falling
     * back to trusting any certificate.
     */
    certSha256: string;
}

export type SystemTime = {
    secs_since_epoch: number
    nanos_since_epoch: number
}

type FileSelector = {
    path: string
    bucket: string
    point_in_time?: SystemTime
    auth?: string
}

type CommandData = {
    action: string
    query?: FileSelector
}

enum Action {
    Preflight = "Preflight",
    SetSyncMetadata = "SetSyncMetadata",
    Backup = "Backup",
    Recover = "Recover",
    Status = "Status",
    List = "List",
    Timeout = "Timeout",
}

type HandshakeData = {
    client_version?: string
}

type AuthenticationData = {
    token: string
}

type SyncMetadata = {
    meta: MetaData[]
}


export type Tag = {
    key: string
    value: string
}

export type MetaData = {
    tags: Tag[]
    timestamp: SystemTime
    bucket: string
    ack_count: number
    nack_count: number
    skip_count: number
    file_list?: string[]
}

export default class Databurg {
    private endpoint: string
    private port: number
    private auth_token: string
    private bucket: string
    private cert_sha256: string
    private client: TLSClient | null = null

    constructor({ endpoint, auth_token, bucket, cert_sha256 }: { endpoint: string, auth_token: string, bucket: string, cert_sha256: string }) {
        this.auth_token = auth_token
        this.endpoint = endpoint
        this.port = 2403
        this.bucket = bucket
        this.cert_sha256 = cert_sha256
    }

    private async connect() {
        if (this.client !== null) return

        let client = new TLSClient({
            host: this.endpoint || '127.0.0.1',
            port: this.port || 2403,
            certSha256: this.cert_sha256,
        });

        // A failure here must propagate: swallowing it would leave this.client
        // null and every later call would fail with a confusing "not connected"
        // instead of the real cause (a pin mismatch, for example).
        await client.connect();
        this.client = client

        if (!(await this.handshake())) {
            throw new Error("Handshake failed")
        }

        if (!(await this.authenticate())) {
            throw new Error("Authentication failed")
        }
    }

    private async handshake() {
        if (!this.client)
            throw new Error("Client not connected")

        let handshake: HandshakeData = {
            client_version: '0.1.0',
        }
        let json = JSON.stringify(handshake)
        let bytes = Buffer.from(json, 'utf8')

        // Send handshake
        return await this.client.sendWaitAck(bytes)
    }

    private async authenticate() {
        if (!this.client)
            throw new Error("Client not connected")

        let auth: AuthenticationData = {
            token: this.auth_token,
        }
        let json = JSON.stringify(auth)
        let bytes = Buffer.from(json, 'utf8')

        // Send authentication
        return await this.client.sendWaitAck(bytes)
    }

    async status(): Promise<SyncMetadata | null> {
        await this.connect()

        const status: CommandData = {
            action: Action.Status,
            query: {
                path: "",
                bucket: this.bucket,
            }
        }

        let json = JSON.stringify(status)
        let bytes = Buffer.from(json, 'utf8')

        if (!(await this.client?.sendWaitAck(bytes))) return null
        let result = await this.client?.read()
        if (!result) return null

        let metadata: SyncMetadata = JSON.parse(result.toString('utf8'))
        metadata.meta = metadata.meta.filter(m => m.ack_count > 0)
        return metadata
    }
}

class TLSClient {
    private socket: tls.TLSSocket | null = null
    private host: string
    private port: number
    private certSha256: string
    private responseEmitter = new EventEmitter()

    constructor(options: TLSClientOptions) {
        this.host = options.host
        this.port = options.port
        this.certSha256 = (options.certSha256 || '').trim().toLowerCase()
    }

    async connect(): Promise<void> {
        if (!/^[0-9a-f]{64}$/.test(this.certSha256)) {
            throw new Error(
                'certSha256 must be the 64 hex characters of the server certificate SHA-256; ' +
                'refusing to connect without a pinned server certificate'
            )
        }

        try {
            return new Promise((resolve, reject) => {
                const options: tls.ConnectionOptions = {
                    host: this.host,
                    port: this.port,
                    // The server presents a self-signed certificate, so a CA
                    // chain cannot verify it. Trust is established by comparing
                    // the certificate against the configured pin below — never
                    // by accepting whatever is presented.
                    rejectUnauthorized: false,
                }
                this.socket = tls.connect(options, () => {
                    const presented = this.socket!.getPeerCertificate()
                    if (!presented || !presented.raw) {
                        this.socket!.destroy()
                        return reject(new Error('Server presented no certificate'))
                    }

                    const actual = crypto.createHash('sha256').update(presented.raw).digest()
                    const expected = Buffer.from(this.certSha256, 'hex')
                    if (actual.length !== expected.length || !crypto.timingSafeEqual(actual, expected)) {
                        this.socket!.destroy()
                        return reject(new Error(
                            `Server certificate pin mismatch: expected ${this.certSha256}, ` +
                            `presented ${actual.toString('hex')}`
                        ))
                    }

                    resolve()
                })
                this.socket!.on('error', (err) => err.code == 'ECONNREFUSED' ? reject(err) : this.responseEmitter.emit('error', err))
                this.socket.on('data', (data) => this.responseEmitter.emit('response', data))
            })
        } catch (error: any) {
            console.error('Failed to connect to Databurg daemon:', error.message)
            throw error
        }
    }

    async sendWaitAck(request: Buffer): Promise<boolean> {
        return (await this.send(request)).toString('utf8') == 'ACK'
    }

    async send(request: Buffer): Promise<Buffer> {
        if (!this.socket)
            throw new Error('Socket is not connected')

        return new Promise((resolve, reject) => {

            const timeout = setTimeout(() => {
                console.error(`Databurg server ${this.host}:${this.port} did not respond in time. Please try again later`)
                reject(Action.Timeout)
            }, REQUEST_TIMEOUT_MS)

            const length = Buffer.alloc(MESSAGE_LENGTH_BYTES)
            length.writeUInt32BE(request.length, 0)
            let message = Buffer.concat([length, request])

            this.responseEmitter.once('response', (response: Buffer) => {
                const responseLength = response.readUInt32BE(0)
                const res = response.subarray(MESSAGE_LENGTH_BYTES, responseLength + MESSAGE_LENGTH_BYTES)
                clearTimeout(timeout)
                resolve(res)
            });

            this.responseEmitter.once('error', (error) => {
                console.log("Error", error)
                clearTimeout(timeout)
                reject(error)
            });

            // Send the request
            this.socket?.write(message)
        })
    }

    async read(): Promise<Buffer> {
        if (!this.socket)
            throw new Error('Socket is not connected')

        return new Promise((resolve, reject) => {
            let len_wait: number | undefined = undefined
            let len_got = 0
            let buffer = Buffer.alloc(0)

            const timeout = setTimeout(() => {
                console.error(`Databurg server ${this.host}:${this.port} did not respond in time. Please try again later`)
                reject(Action.Timeout)
            }, 10_000)

            this.responseEmitter.once('response', (response: Buffer) => {
                let current_res: Buffer;
                if (len_wait === undefined) {
                    current_res = response.subarray(MESSAGE_LENGTH_BYTES, response.length + MESSAGE_LENGTH_BYTES)
                    len_wait = response.readUInt32BE(0)
                } else
                    current_res = response

                len_got += current_res.length
                buffer = Buffer.concat([buffer, current_res])

                clearTimeout(timeout)

                if (len_wait == len_got)
                    resolve(buffer)
            });

            this.responseEmitter.once('error', (error) => {
                console.log("Error", error)
                clearTimeout(timeout)
                reject(error)
            });
        });
    }

    close() {
        if (this.socket) {
            this.socket.end()
            this.socket = null
        }
    }
}