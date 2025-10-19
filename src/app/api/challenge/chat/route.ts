import { NextRequest } from "next/server";
import OpenAI from "openai";
import { supabase } from "@/lib/supabase";

export async function POST(req: NextRequest) {
    // Create a TransformStream for streaming the response
    const encoder = new TextEncoder();
    const stream = new TransformStream();
    const writer = stream.writable.getWriter();
    const reqId = Math.random().toString(36).slice(2, 10);
    const startTs = Date.now();
    console.log(`[chat][${reqId}] POST /api/challenge/chat start`);

    const streamResponse = async () => {
        try {
            const body = await req.json();
            console.log(
                `[chat][${reqId}] parsed body: name=%s inputLen=%d`,
                body?.name,
                (body?.input ?? "").length,
            );
            const challengeName = body.name;

            if (challengeName === undefined || challengeName === "") {
                console.warn(`[chat][${reqId}] empty challenge name`);
                await writer.write(
                    encoder.encode(
                        `data: ${JSON.stringify({ content: "Please select a level to start." })}\n\n`,
                    ),
                );
                await writer.write(encoder.encode("data: [DONE]\n\n"));
                await writer.close();
                return;
            }

            // Fetch challenge from Supabase
            const { data: challenge, error } = await supabase
                .from("challenges")
                .select("system_prompt, answer")
                .eq("name", challengeName)
                .single();
            console.log(
                `[chat][${reqId}] supabase fetched: error=%s hasChallenge=%s`,
                error?.message ?? null,
                !!challenge,
            );

            // Handle errors
            if (error || !challenge) {
                await writer.write(
                    encoder.encode(
                        `data: ${JSON.stringify({ content: "Challenge not found" })}\n\n`,
                    ),
                );
                await writer.write(encoder.encode("data: [DONE]\n\n"));
                await writer.close();
                console.warn(
                    `[chat][${reqId}] challenge not found: %s`,
                    challengeName,
                );
                return;
            }

            const { system_prompt, answer } = challenge;
            const systemPrompt = system_prompt.replaceAll("█████", answer);
            console.log(
                `[chat][${reqId}] prompt prepared: systemLen=%d answerLen=%d`,
                (systemPrompt ?? "").length,
                (answer ?? "").length,
            );

            // Initialize OpenAI-compatible client for Gemini (Google AI Studio)
            const googleApiKey = process.env.GEMINI_API_KEY;
            if (!googleApiKey) {
                console.error(`[chat][${reqId}] missing GEMINI_API_KEY`);
                await writer.write(
                    encoder.encode(
                        `data: ${JSON.stringify({
                            error: "Server misconfiguration: missing GEMINI_API_KEY",
                        })}\n\n`,
                    ),
                );
                await writer.write(encoder.encode("data: [DONE]\n\n"));
                await writer.close();
                return;
            }

            const client = new OpenAI({
                apiKey: googleApiKey,
                // OpenAI compatibility endpoint for Gemini (no trailing slash)
                baseURL:
                    "https://generativelanguage.googleapis.com/v1beta/openai",
                // Ensure Google accepts the key header in compatibility mode
                defaultHeaders: { "x-goog-api-key": googleApiKey },
            });
            console.log(
                `[chat][${reqId}] OpenAI client ready (compat). model=%s`,
                "gemini-2.0-flash-lite",
            );

            // Create streaming completion
            const completion = await client.chat.completions.create({
                // Use Google AI Studio's OpenAI-compatible alias for Gemini 1.5 Flash
                model: "gemini-2.0-flash-lite",
                messages: [
                    {
                        role: "system",
                        content: systemPrompt,
                    },
                    {
                        role: "user",
                        content: body.input,
                    },
                ],
                stream: true, // Enable streaming
            });

            // Process the streaming response
            let chunks = 0;
            let totalChars = 0;
            for await (const chunk of completion) {
                const content = chunk.choices[0]?.delta?.content || "";
                if (content) {
                    chunks++;
                    totalChars += content.length;
                    if (chunks <= 2 || chunks % 25 === 0) {
                        console.log(
                            `[chat][${reqId}] stream chunk #%d len=%d totalChars=%d`,
                            chunks,
                            content.length,
                            totalChars,
                        );
                    }
                    await writer.write(
                        encoder.encode(
                            `data: ${JSON.stringify({ content })}\n\n`,
                        ),
                    );
                }
            }

            await writer.write(encoder.encode("data: [DONE]\n\n"));
            await writer.close();
            console.log(
                `[chat][${reqId}] stream finished after %d chunks, %d chars, %dms`,
                chunks,
                totalChars,
                Date.now() - startTs,
            );
        } catch (error) {
            const err: any = error;
            console.error(
                `[chat][${reqId}] error: name=%s code=%s status=%s respStatus=%s msg=%s`,
                err?.name,
                err?.code,
                err?.status,
                err?.response?.status,
                err?.message,
            );
            if (err?.response?.status === 404) {
                console.error(
                    `[chat][${reqId}] Hint: 404 from compat endpoint. Verify baseURL=https://generativelanguage.googleapis.com/v1beta/openai and model=gpt-4o-mini`,
                );
            }
            if (err?.response?.data) {
                try {
                    console.error(
                        `[chat][${reqId}] error response data: %s`,
                        JSON.stringify(err.response.data),
                    );
                } catch (_) {
                    console.error(
                        `[chat][${reqId}] error response data (non-JSON)`,
                    );
                }
            }
            const errorMessage = (error as Error).message;
            await writer.write(
                encoder.encode(
                    `data: ${JSON.stringify({ error: errorMessage })}\n\n`,
                ),
            );
            await writer.write(encoder.encode("data: [DONE]\n\n"));
            await writer.close();
            console.log(
                `[chat][${reqId}] error streamed to client, %dms`,
                Date.now() - startTs,
            );
        }
    };

    // Execute the streaming function
    streamResponse();

    // Return the readable stream as the response
    return new Response(stream.readable, {
        headers: {
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            Connection: "keep-alive",
        },
    });
}
