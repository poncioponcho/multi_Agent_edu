import katex from 'katex'

/**
 * 把 LLM 回复中的数学公式片段渲染为 KaTeX HTML，其余文本做 HTML 转义。
 *
 * 支持的定界符（DeepSeek 常见输出）：
 *   行内：\( ... \)  或  $ ... $
 *   独立：\[ ... \]  或  $$ ... $$
 *
 * 转义陷阱：LLM 可能输出双反斜杠 \\(（JSON 转义残留），正则用 \\+ 兼容。
 */
function escapeHtml(s: string): string {
  return s
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
}

const MATH_SEGMENT_RE =
  /(\$\$[\s\S]+?\$\$|\\+\[[\s\S]+?\\+\]|\\+\([\s\S]+?\\+\)|\$[^$\n]+?\$)/g

function renderSegment(seg: string): string {
  let tex = ''
  let displayMode = false
  if (seg.startsWith('$$')) {
    tex = seg.slice(2, -2)
    displayMode = true
  } else if (/^\\+\[/.test(seg)) {
    tex = seg.slice(2, -2)
    displayMode = true
  } else if (/^\\+\(/.test(seg)) {
    tex = seg.slice(2, -2)
  } else {
    tex = seg.slice(1, -1)
  }
  try {
    return katex.renderToString(tex.trim(), {
      throwOnError: false,
      displayMode,
      output: 'html',
    })
  } catch {
    return escapeHtml(seg)
  }
}

export function renderRichText(raw: string): string {
  const parts: string[] = []
  let last = 0
  for (const m of raw.matchAll(MATH_SEGMENT_RE)) {
    const idx = m.index ?? 0
    parts.push(escapeHtml(raw.slice(last, idx)))
    parts.push(renderSegment(m[0]))
    last = idx + m[0].length
  }
  parts.push(escapeHtml(raw.slice(last)))
  return parts.join('')
}
