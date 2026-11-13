import { faq, plainAnswer } from "@/lib/faq";
import { routeDocs } from "@/lib/routes";
import { docsPath, site } from "@/lib/site";

/**
 * Schema.org JSON-LD for every page, as one connected graph.
 *
 * Each entity has a stable `@id` (URL plus fragment) and refers to the others
 * by it: the site is published by the Association, the software is documented
 * by the API reference, every page is part of the site. Pages add their own
 * nodes and point back at these shared ones.
 */

const abs = (path: string): string => `${site.url}${path}`;

export const ids = {
  organization: `${site.url}/#organization`,
  website: `${site.url}/#website`,
  software: `${site.url}/#software`,
  api: abs(`${docsPath}#webapi`),
} as const;

const organization = {
  "@type": "Organization",
  "@id": ids.organization,
  name: site.association,
  url: site.parent,
  logo: abs("/icon-512.png"),
  sameAs: ["https://github.com/bsv-blockchain"],
};

const website = {
  "@type": "WebSite",
  "@id": ids.website,
  name: site.name,
  url: abs("/"),
  description: site.description,
  inLanguage: "en",
  publisher: { "@id": ids.organization },
};

const software = {
  "@type": "SoftwareApplication",
  "@id": ids.software,
  name: site.name,
  description: site.description,
  url: abs("/"),
  applicationCategory: "DeveloperApplication",
  operatingSystem: "Linux, macOS, Docker",
  isAccessibleForFree: true,
  offers: { "@type": "Offer", price: "0", priceCurrency: "USD" },
  license: `${site.repo}/blob/main/LICENSE`,
  sameAs: [site.repo],
  publisher: { "@id": ids.organization },
  author: { "@id": ids.organization },
};

/** JSON-LD as a script body, with `<` escaped so no string can close the tag. */
export function serialize(data: object): string {
  return JSON.stringify(data).replace(/</g, "\\u003c");
}

/** Front page: the shared graph, the page itself, and the FAQ shown on it. */
export function homeGraph(): object {
  return {
    "@context": "https://schema.org",
    "@graph": [
      organization,
      website,
      software,
      {
        "@type": "WebPage",
        "@id": abs("/#webpage"),
        url: abs("/"),
        name: `${site.name}: transaction broadcast manager API`,
        description: site.description,
        inLanguage: "en",
        isPartOf: { "@id": ids.website },
        about: { "@id": ids.software },
        author: { "@id": ids.organization },
        datePublished: site.published,
        dateModified: site.updated,
      },
      {
        "@type": "FAQPage",
        "@id": abs("/#faq"),
        isPartOf: { "@id": abs("/#webpage") },
        mainEntity: faq.map((item) => ({
          "@type": "Question",
          name: item.question,
          acceptedAnswer: { "@type": "Answer", text: plainAnswer(item.answer) },
        })),
      },
    ],
  };
}

/** API reference: the shared graph, the article, the API it documents, and the breadcrumb. */
export function docsGraph(): object {
  const url = abs(docsPath);
  return {
    "@context": "https://schema.org",
    "@graph": [
      organization,
      website,
      software,
      {
        "@type": "TechArticle",
        "@id": `${url}#article`,
        url,
        headline: "BSV Broadcaster API reference",
        description:
          "Every BSV Broadcaster API server route with its request and response contract, headers and examples, plus the Chaintracks block-header API.",
        inLanguage: "en",
        proficiencyLevel: "Expert",
        isPartOf: { "@id": ids.website },
        about: [{ "@id": ids.software }, { "@id": ids.api }],
        author: { "@id": ids.organization },
        publisher: { "@id": ids.organization },
        datePublished: site.published,
        dateModified: site.updated,
        breadcrumb: { "@id": `${url}#breadcrumb` },
      },
      {
        "@type": "WebAPI",
        "@id": ids.api,
        name: "BSV Broadcaster API",
        description: `HTTP API for submitting BSV transactions and tracking their status: ${routeDocs.length} API server routes plus Server-Sent Events and the Chaintracks header API.`,
        url: site.apiBaseUrl,
        documentation: url,
        termsOfService: `${site.repo}/blob/main/LICENSE`,
        provider: { "@id": ids.organization },
        isRelatedTo: { "@id": ids.software },
      },
      {
        "@type": "BreadcrumbList",
        "@id": `${url}#breadcrumb`,
        itemListElement: [
          { "@type": "ListItem", position: 1, name: "Broadcaster", item: abs("/") },
          { "@type": "ListItem", position: 2, name: "API reference", item: url },
        ],
      },
    ],
  };
}
