"use client";

import { redirect } from "next/navigation";

export default function Home() {
  redirect("/data-sources");
  return null;
}
