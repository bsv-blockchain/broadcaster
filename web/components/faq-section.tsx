import type { ReactNode } from "react";
import { Reveal, RevealGroup, RevealItem } from "@/components/reveal";
import { TermChip } from "@/components/term-chip";
import { Eyebrow, Section } from "@/components/section";
import { faq } from "@/lib/faq";

/**
 * `backtick` spans as code chips (linked, with a tooltip, when the glossary knows them) and **asterisk** spans as highlighted terms, so
 * a skimming eye lands on the words that answer the question.
 */
function Answer({ text }: { text: string }): ReactNode {
  return text.split(/(`[^`]+`|\*\*[^*]+\*\*)/g).map((part, i) => {
    if (part.startsWith("`") && part.endsWith("`")) {
      return <TermChip key={i} term={part.slice(1, -1)} />;
    }
    if (part.startsWith("**") && part.endsWith("**")) {
      return (
        <strong key={i} className="font-semibold text-foreground">
          {part.slice(2, -2)}
        </strong>
      );
    }
    return part;
  });
}

/**
 * The questions integrators ask, each answered in a few sentences.
 *
 * Every answer is in the HTML, open, rather than behind an accordion: these are
 * short, and a reader (or an assistant quoting the page) should not need to
 * click to get one. The same items are published as FAQPage JSON-LD.
 */
export function FaqSection(): ReactNode {
  return (
    <Section id="faq" aria-labelledby="faq-heading" className="scroll-mt-20">
      <Reveal className="mb-10 max-w-2xl md:mb-14">
        <Eyebrow>Questions</Eyebrow>
        <h2 id="faq-heading" className="headline-lg">
          How does BSV Broadcaster work?
        </h2>
      </Reveal>
      <RevealGroup className="grid gap-px overflow-hidden rounded-lg border border-border bg-border md:grid-cols-2">
        {faq.map((item) => (
          <RevealItem key={item.id} className="bg-surface-raised">
            <article id={item.id} className="h-full scroll-mt-24 p-6 md:p-7">
              <h3 className="headline-sm">{item.question}</h3>
              <p className="mt-3 max-w-prose text-[1.0625rem] leading-[1.75] text-pretty text-foreground/75">
                <Answer text={item.answer} />
              </p>
            </article>
          </RevealItem>
        ))}
      </RevealGroup>
    </Section>
  );
}
