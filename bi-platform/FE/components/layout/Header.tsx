"use client";

import { Menu, ShieldCheck } from "lucide-react";
import { Button } from "@/components/ui/button";
import { useAuth } from "@/lib/AuthContext";
import { useRouter } from "next/navigation";

interface HeaderProps {
    onMenuClick?: () => void;
}

export function Header({ onMenuClick }: HeaderProps) {
    const { isAuthenticated, user, logout, openAuthModal } = useAuth();
    const router = useRouter();

    return (
        <header className="sticky top-0 z-30 flex h-16 items-center gap-4 border-b bg-background/95 px-6 backdrop-blur supports-[backdrop-filter]:bg-background/60">
            <div className="flex flex-1 items-center gap-4">
                {onMenuClick && (
                    <button className="lg:hidden p-2 -ml-2 hover:bg-muted rounded-md" onClick={onMenuClick}>
                        <Menu className="h-6 w-6" />
                    </button>
                )}
            </div>

            <div className="flex items-center gap-3">
                <div className="flex items-center gap-2 pl-2">
                    {isAuthenticated && user ? (
                        <>
                            <div className="h-8 w-8 rounded-full bg-primary/10 flex items-center justify-center text-primary font-medium overflow-hidden">
                                {user.avatar_url ? (
                                    <img src={user.avatar_url} alt={user.full_name || 'User'} className="w-full h-full object-cover" />
                                ) : (
                                    (user.full_name?.[0] || user.email[0]).toUpperCase()
                                )}
                            </div>
                            <div className="hidden md:block text-sm">
                                <p className="font-medium leading-none">{user.full_name || user.email.split('@')[0]}</p>
                                <p className="text-xs text-muted-foreground capitalize">{user.role}</p>
                            </div>
                            {user?.role === 'admin' && (
                                <Button variant="outline" size="sm" onClick={() => router.push('/admin')} className="ml-2 hidden lg:flex items-center gap-1 border-primary/50 text-primary hover:bg-primary/10">
                                    <ShieldCheck className="h-4 w-4" />
                                    <span className="hidden xl:inline">Quản Trị</span>
                                </Button>
                            )}
                            <Button variant="ghost" size="icon" onClick={() => logout()} title="Đăng xuất" className="ml-2 h-8 w-8 text-muted-foreground hover:text-red-500">
                                <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"></path><polyline points="16 17 21 12 16 7"></polyline><line x1="21" y1="12" x2="9" y2="12"></line></svg>
                            </Button>
                        </>
                    ) : (
                        <Button onClick={openAuthModal} size="sm" className="ml-2">
                            Đăng Nhập
                        </Button>
                    )}
                </div>
            </div>
        </header>
    );
}
