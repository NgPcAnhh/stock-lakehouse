'use client';

import React, { createContext, useContext, useEffect, useState, ReactNode } from 'react';
import { usePathname } from 'next/navigation';
import { User, getAccessToken, clearTokens, fetchWithAuth } from './auth';

const JUST_LOGGED_IN_KEY = 'stockpro:auth:just-logged-in';

interface AuthContextType {
    user: User | null;
    isAuthenticated: boolean;
    isLoading: boolean;
    login: (user: User) => void;
    logout: () => void;
    openAuthModal: () => void;
    closeAuthModal: () => void;
    isAuthModalOpen: boolean;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

export const AuthProvider = ({ children }: { children: ReactNode }) => {
    const [user, setUser] = useState<User | null>(null);
    const [isLoading, setIsLoading] = useState(true);
    const [isAuthModalOpen, setIsAuthModalOpen] = useState(false);
    const pathname = usePathname();

    useEffect(() => {
        const loadUser = async () => {
            // Dù có access_token hay refresh_token đều thử load /me
            const token = getAccessToken();
            const refresh = typeof window !== 'undefined' ? localStorage.getItem('refresh_token') || document.cookie.includes('refresh_token') : false;

            if (!token && !refresh) {
                setIsLoading(false);
                return;
            }

            try {
                const res = await fetchWithAuth('http://localhost:8000/api/v1/auth/me');
                if (res.ok) {
                    const userData = await res.json();
                    setUser(userData);
                } else {
                    clearTokens();
                    setUser(null);
                }
            } catch (err) {
                clearTokens();
                setUser(null);
            } finally {
                setIsLoading(false);
            }
        };

        loadUser();
    }, []);

    useEffect(() => {
        if (!user) return;

        const IDLE_TIMEOUT = 5 * 60 * 1000; // 5 minutes
        const RENEW_THRESHOLD = 4 * 60 * 1000; // Renew session every 4 minutes if active
        const CHECK_INTERVAL = 10000; // Check every 10 seconds

        let lastActivity = Date.now();
        let lastPing = Date.now();

        const updateActivity = () => {
            lastActivity = Date.now();
        };

        const events = ['mousemove', 'keydown', 'scroll', 'click', 'touchstart'];
        events.forEach(event => window.addEventListener(event, updateActivity, { passive: true }));

        const managementInterval = setInterval(async () => {
            const now = Date.now();
            const idleTime = now - lastActivity;

            // 1. Check for Inactivity Timeout
            if (idleTime >= IDLE_TIMEOUT) {
                // Tự động gọi API đăng xuất và xoá token
                const refresh = typeof window !== 'undefined' && document.cookie.split('; ').find(row => row.startsWith('refresh_token='))?.split('=')[1];
                if (refresh) {
                    fetch('http://localhost:8000/api/v1/auth/logout', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ refresh_token: refresh })
                    }).catch(() => {});
                }
                clearTokens();
                setUser(null);
                return;
            }

            // 2. Proactive Session Renewal (Gia hạn session)
            // If user is active AND it's been more than RENEW_THRESHOLD since last ping
            if (idleTime < IDLE_TIMEOUT && (now - lastPing) >= RENEW_THRESHOLD) {
                lastPing = now;
                try {
                    // Gọi endpoint /me để refresh token và cập nhật session trên server
                    await fetchWithAuth('http://localhost:8000/api/v1/auth/me');
                } catch (err) {
                    // Ignore background fetch errors
                }
            }
        }, CHECK_INTERVAL);

        return () => {
            events.forEach(event => window.removeEventListener(event, updateActivity));
            clearInterval(managementInterval);
        };
    }, [user]);

    const login = (newUser: User) => {
        setUser(newUser);
        try {
            sessionStorage.setItem(JUST_LOGGED_IN_KEY, '1');
        } catch {
            // ignore storage failures
        }
    };

    const logout = async () => {
        // Optionally alert Server to revoke refresh token
        const refresh = typeof window !== 'undefined' && document.cookie.split('; ').find(row => row.startsWith('refresh_token='))?.split('=')[1];

        if (refresh) {
            try {
                await fetch('http://localhost:8000/api/v1/auth/logout', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ refresh_token: refresh })
                });
            } catch (e) {
                // ignore error on logout
            }
        }

        clearTokens();
        setUser(null);
        try {
            sessionStorage.removeItem(JUST_LOGGED_IN_KEY);
        } catch {
            // ignore storage failures
        }
    };

    return (
        <AuthContext.Provider
            value={{
                user,
                isAuthenticated: !!user,
                isLoading,
                login,
                logout,
                openAuthModal: () => setIsAuthModalOpen(true),
                closeAuthModal: () => setIsAuthModalOpen(false),
                isAuthModalOpen,
            }}
        >
            {children}
        </AuthContext.Provider>
    );
};

export const useAuth = () => {
    const context = useContext(AuthContext);
    if (context === undefined) {
        throw new Error('useAuth must be used within an AuthProvider');
    }
    return context;
};
