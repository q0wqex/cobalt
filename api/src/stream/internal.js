import { request } from "undici";
import { Readable } from "node:stream";
import { closeRequest, getHeaders, pipe } from "./shared.js";
import { handleHlsPlaylist, isHlsResponse, probeInternalHLSTunnel } from "./internal-hls.js";

const CHUNK_SIZE = BigInt(8e6); // 8 MB

const serviceNeedsChunks = new Set(["vk"]);

async function* readChunks(streamInfo, size) {
    let read = 0n, chunksSinceTransplant = 0;
    while (read < size) {
        if (streamInfo.controller.signal.aborted) {
            throw new Error("controller aborted");
        }

        const toByte = (read + CHUNK_SIZE - 1n < size) ? (read + CHUNK_SIZE - 1n) : (size - 1n);

        const chunk = await request(streamInfo.url, {
            headers: {
                ...getHeaders(streamInfo.service),
                Range: `bytes=${read}-${toByte}`
            },
            dispatcher: streamInfo.dispatcher,
            signal: streamInfo.controller.signal,
            maxRedirections: 4
        });

        if (chunk.statusCode === 403 && chunksSinceTransplant >= 3 && streamInfo.transplant) {
            chunksSinceTransplant = 0;
            try {
                await streamInfo.transplant(streamInfo.dispatcher);
                continue;
            } catch {}
        }

        if (chunk.statusCode !== 200 && chunk.statusCode !== 206) {
            closeRequest(streamInfo.controller);
            break;
        }

        chunksSinceTransplant++;

        let chunkBytes = 0n;
        for await (const data of chunk.body) {
            chunkBytes += BigInt(data.length);
            yield data;
        }

        if (chunkBytes === 0n) {
            break;
        }

        read += chunkBytes;
    }
}

async function handleChunkedStream(streamInfo, res) {
    const { signal } = streamInfo.controller;
    const cleanup = () => (res.end(), closeRequest(streamInfo.controller));

    try {
        let req, attempts = 3, size = 0n;

        try {
            const parsedUrl = new URL(streamInfo.url);
            const clen = parsedUrl.searchParams.get('clen');
            if (clen) {
                size = BigInt(clen);
            }
        } catch {}

        while (attempts--) {
            req = await fetch(streamInfo.url, {
                headers: {
                    ...getHeaders(streamInfo.service),
                    Range: 'bytes=0-0'
                },
                method: 'GET',
                dispatcher: streamInfo.dispatcher,
                signal
            });

            streamInfo.url = req.url;
            if (req.status === 403 && streamInfo.transplant) {
                try {
                    await streamInfo.transplant(streamInfo.dispatcher);
                } catch {
                    break;
                }
            } else break;
        }

        if (!size) {
            const contentRange = req.headers.get('content-range');
            if (contentRange) {
                const total = contentRange.split('/')[1];
                if (total && total !== '*') {
                    size = BigInt(total);
                }
            }
            if (!size) {
                const cl = req.headers.get('content-length');
                if (cl) size = BigInt(cl);
            }
        }

        if (!size || (req.status !== 200 && req.status !== 206)) {
            return cleanup();
        }

        const generator = readChunks(streamInfo, size);

        const abortGenerator = () => {
            generator.return();
            signal.removeEventListener('abort', abortGenerator);
        }

        signal.addEventListener('abort', abortGenerator);

        const stream = Readable.from(generator);

        const contentType = req.headers.get('content-type');
        if (contentType) res.setHeader('content-type', contentType);
        res.setHeader('content-length', size.toString());

        pipe(stream, res, cleanup);
    } catch {
        cleanup();
    }
}

async function handleGenericStream(streamInfo, res) {
    const { signal } = streamInfo.controller;
    const cleanup = () => res.end();

    try {
        const fileResponse = await request(streamInfo.url, {
            headers: {
                ...getHeaders(streamInfo.service),
                ...(streamInfo.headers ? Object.fromEntries(streamInfo.headers) : {}),
                host: undefined
            },
            dispatcher: streamInfo.dispatcher,
            signal,
            maxRedirections: 16
        });

        res.status(fileResponse.statusCode);
        fileResponse.body.on('error', () => {});

        const isHls = isHlsResponse(fileResponse, streamInfo);

        for (const [ name, value ] of Object.entries(fileResponse.headers)) {
            if (!isHls || name.toLowerCase() !== 'content-length') {
                res.setHeader(name, value);
            }
        }

        if (fileResponse.statusCode < 200 || fileResponse.statusCode > 299) {
            return cleanup();
        }

        if (isHls) {
            await handleHlsPlaylist(streamInfo, fileResponse, res);
        } else {
            pipe(fileResponse.body, res, cleanup);
        }
    } catch {
        closeRequest(streamInfo.controller);
        cleanup();
    }
}

export function internalStream(streamInfo, res) {
    if (streamInfo.headers) {
        streamInfo.headers.delete('icy-metadata');
    }

    if (serviceNeedsChunks.has(streamInfo.service) && !streamInfo.isHLS) {
        return handleChunkedStream(streamInfo, res);
    }

    return handleGenericStream(streamInfo, res);
}

export async function probeInternalTunnel(streamInfo) {
    try {
        const signal = AbortSignal.timeout(3000);
        const headers = {
            ...Object.fromEntries(streamInfo.headers || []),
            ...getHeaders(streamInfo.service),
            host: undefined,
            range: undefined
        };

        if (streamInfo.isHLS) {
            return probeInternalHLSTunnel({
                ...streamInfo,
                signal,
                headers
            });
        }

        let size;
        try {
            const parsedUrl = new URL(streamInfo.url);
            const clen = parsedUrl.searchParams.get('clen');
            if (clen) size = +clen;
        } catch {}

        if (size && !isNaN(size) && size > 0) return size;

        const response = await request(streamInfo.url, {
            method: 'GET',
            headers: {
                ...headers,
                Range: 'bytes=0-0'
            },
            dispatcher: streamInfo.dispatcher,
            signal,
            maxRedirections: 16
        });

        if (response.statusCode !== 200 && response.statusCode !== 206)
            throw "status is not 200/206";

        const contentRange = response.headers['content-range'];
        if (contentRange) {
            const total = String(contentRange).split('/')[1];
            if (total && total !== '*') {
                size = +total;
            }
        }

        if (!size) {
            size = +response.headers['content-length'];
        }

        if (isNaN(size) || size <= 0)
            throw "content-length is not a number";

        return size;
    } catch {}
}
