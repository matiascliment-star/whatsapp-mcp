// ============================================================
// WA MCP — Audio Conversion Service (ffmpeg)
// Converts any audio format to OGG Opus for WhatsApp compatibility.
// ============================================================

import { execFile } from "node:child_process";
import { writeFile, unlink, readFile } from "node:fs/promises";
import { join } from "node:path";
import { randomUUID } from "node:crypto";
import { existsSync, mkdirSync } from "node:fs";

const TMP_DIR = "data/media/tmp";

if (!existsSync(TMP_DIR)) {
  mkdirSync(TMP_DIR, { recursive: true });
}

/**
 * Convert an audio buffer to OGG Opus format using ffmpeg.
 * Returns the converted buffer, or the original if ffmpeg is not available.
 */
export async function convertToOggOpus(input: Buffer): Promise<Buffer> {
  const id = randomUUID();
  const inputPath = join(TMP_DIR, `${id}_in`);
  const outputPath = join(TMP_DIR, `${id}_out.ogg`);

  try {
    await writeFile(inputPath, input);

    await new Promise<void>((resolve, reject) => {
      execFile(
        "ffmpeg",
        [
          "-i", inputPath,
          "-vn",                    // no video
          "-ar", "48000",           // 48kHz sample rate (Opus standard)
          "-ac", "1",               // mono
          "-c:a", "libopus",        // Opus codec
          "-b:a", "64k",            // 64kbps bitrate
          "-application", "voip",   // optimized for voice
          "-y",                     // overwrite
          outputPath,
        ],
        { timeout: 30000 },
        (err, _stdout, stderr) => {
          if (err) {
            reject(new Error(`ffmpeg failed: ${stderr || err.message}`));
          } else {
            resolve();
          }
        },
      );
    });

    const converted = await readFile(outputPath);
    return converted;
  } finally {
    // Cleanup temp files
    await unlink(inputPath).catch(() => {});
    await unlink(outputPath).catch(() => {});
  }
}
