import NextAuth from "next-auth";
import MicrosoftEntraID from "next-auth/providers/microsoft-entra-id";

const allowedEmails = new Set(
  (process.env.ALLOWED_EMAILS ?? "")
    .split(",")
    .map((e) => e.trim().toLowerCase())
    .filter(Boolean)
);

const tenantId = process.env.AUTH_MICROSOFT_ENTRA_ID_TENANT_ID!;

export const { handlers, signIn, signOut, auth } = NextAuth({
  providers: [
    MicrosoftEntraID({
      clientId: process.env.AUTH_MICROSOFT_ENTRA_ID_ID,
      clientSecret: process.env.AUTH_MICROSOFT_ENTRA_ID_SECRET,
      issuer: `https://login.microsoftonline.com/${tenantId}/v2.0`,
      authorization: {
        url: `https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/authorize`,
        params: { scope: "openid profile email" },
      },
      token: `https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/token`,
      userinfo: "https://graph.microsoft.com/oidc/userinfo",
    }),
  ],
  pages: { signIn: "/login" },
  callbacks: {
    signIn({ user }) {
      if (!allowedEmails.size) return true;
      return allowedEmails.has(user.email?.toLowerCase() ?? "");
    },
  },
});
